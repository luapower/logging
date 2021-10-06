
--file and tcp logging with capped disk & memory usage.
--Written by Cosmin Apreutesei. Public domain.

local queue = require'queue'
local pp = require'pp'
local _ = string.format

local M = {}

function M.file_logger(logfile_template, max_size)

	local fs = require'fs'

	local logfile  = _(logfile_template, '')
	local logfile0 = _(logfile_template, '0')

	local f, size

	local function check(s, ret, err)
		if ret then return ret end
		io.stderr:write(_('file_logger %s error: %s\n', s, err))
		if f then f:close(); f = nil end
	end

	local function open()
		if f then return true end
		f = check('fs.open()', fs.open(logfile, 'a')); if not f then return end
		size = check("f:attr'size'", f:attr'size'); if not f then return end
		return true
	end

	local function rotate(len)
		if max_size and size + len > max_size / 2 then
			f:close(); f = nil
			if not check('fs.move()', fs.move(logfile, logfile0)) then return end
			if not open() then return end
		end
		return true
	end

	return function(s)
		if not open() then return end
		if not rotate(#s + 1) then return end
		size = size + #s + 1
		if not check('write', f:write(s..'\n')) then return end
		if not check('flush', f:flush()) then return end
	end
end

function M.tcp_logger(host, port, max_len)

	local sock = require'sock'

	local tcp
	local queue = queue(max_len or 1/0)

	local function check(s, ret, err)
		if ret then return ret end
		io.stderr:write(_('tcp_logger %s error: %s\n', s, err))
	end

	local function check_io(s, ret, err)
		if ret then return ret end
		check(s, ret, err)
		if tcp then tcp:close(); tcp = nil end
	end

	local function connect()
		if tcp then return tcp end
		tcp = check_io('sock.tcp()', sock.tcp()); if not tcp then return end
		if not check_io('tcp:connect()', tcp:connect(host, port)) then return end
		return true
	end

	local send_thread_suspended = true
	local send_thread = sock.newthread(function()
		local lenbuf = ffi.new'int[1]'
		while true do
			local msg = queue:peek()
			if msg then
				if connect() then
					local s = pp.format(msg)
					lenbuf[0] = #s
					local len = ffi.string(lenbuf, ffi.sizeof(lenbuf))
					if check('tcp:send()', tcp:send(len..s)) then
						queue:pop()
					end
				end
			else
				send_thread_suspended = true
				sock.suspend()
				send_thread_suspended = false
			end
		end
	end)

	return function(msg)
		if not check('queue:push()', queue:push(msg)) then
			queue:pop()
			queue:push(msg)
		end
		if send_thread_suspended then
			sock.resume(send_thread)
		end
	end
end

if not ... then

	local log = M.file_logger('test%s.log', 4000)
	for i=1,1000 do
		log(_('some event %d', i))
	end

end

return M
