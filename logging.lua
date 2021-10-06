--[[

	file and tcp logging with capped disk & memory usage.
	Written by Cosmin Apreutesei. Public domain.

	logging.log(severity, module, event, fmt, ...)
	logging.note(module, event, fmt, ...)
	logging.nolog(module, event, fmt, ...)
	logging.dbg(module, event, fmt, ...)
	logging.warnif(module, event, condition, fmt, ...)
	logging.logerror(module, event, fmt, ...)

	logging.args(...) -> ...

	debug.env <- 'dev' | 'prod', etc.
	logging.filter <- {severity->true}

	logging:tofile(logfile, max_disk_size)
	logging:toserver(host, port, queue_size, timeout)

]]

local ffi = require'ffi'
local queue = require'queue'
local time = require'time'
local clock = time.clock
local time = time.time
local pp = require'pp'
local _ = string.format

local logging = {}

function logging:tofile(logfile, max_size)

	local fs = require'fs'

	local logfile0 = logfile:gsub('(%.[^%.]+)$', '0%1')
	if logfile0 == logfile then logfile0 = logfile..'0' end

	local f, size

	local function check(s, ret, err)
		if ret then return ret end
		io.stderr:write(_('file_logging %s error: %s\n', s, err))
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

	function self:logtofile(s)
		if not open() then return end
		if not rotate(#s + 1) then return end
		size = size + #s + 1
		if not check('write', f:write(s)) then return end
		if not check('flush', f:flush()) then return end
	end

	return self
end

function logging:toserver(host, port, queue_size, timeout)

	local sock = require'sock'

	local tcp
	local queue = queue.new(queue_size or 1/0)

	local function check(s, ret, err)
		if ret then return ret end
		io.stderr:write(_('tcp_logging %s error: %s\n', s, err))
	end

	local function check_io(s, ret, err)
		if ret then return ret end
		check(s, ret, err)
		if tcp then tcp:close(); tcp = nil end
	end

	local function connect()
		if tcp then return tcp end
		tcp = check_io('sock.tcp()', sock.tcp()); if not tcp then return end
		local exp = timeout and clock() + timeout
		if not check_io('tcp:connect()', tcp:connect(host, port, exp)) then return end
		return true
	end

	local send_thread_suspended = true
	local send_thread = sock.newthread(function()
		send_thread_suspended = false
		local lenbuf = ffi.new'int[1]'
		while true do
			local msg = queue:peek()
			if msg then
				if connect() then
					local s = pp.format(msg)
					lenbuf[0] = #s
					local len = ffi.string(lenbuf, ffi.sizeof(lenbuf))
					local exp = timeout and clock() + timeout
					if check('tcp:send()', tcp:send(len..s, nil, exp)) then
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

	function self:logtoserver(msg)
		if not check('queue:push()', queue:push(msg)) then
			queue:pop()
			queue:push(msg)
		end
		if send_thread_suspended then
			sock.resume(send_thread)
		end
	end

	return self
end

logging.filter = {}
debug.env = debug.env or 'dev'

local names = setmetatable({}, {__mode = 'k'}) --{[obj]->name}

function logging.name(obj, name)
	names[obj] = name
end

logging.name(coroutine.running(), 'TM')

local function has_fields(v)
	return type(v) == 'table' or type(v) == 'cdata'
end

local function debug_type(v)
	return has_fields(v) and v.type or type(v)
end

local prefixes = {
	thread = 'T',
	['function'] = 'f',
}

local function debug_prefix(v)
	return has_fields(v) and v.debug_prefix or prefixes[debug_type(v)]
end

local ids_db = {} --{type->{last_id=,[obj]->id}}

local function debug_id(v)
	local type = debug_type(v)
	local ids = ids_db[type]
	if not ids then
		ids = setmetatable({}, {__mode = 'k'})
		ids_db[type] = ids
	end
	local id = ids[v]
	if not id then
		id = (ids.last_id or 0) + 1
		ids.last_id = id
		ids[v] = id
	end
	return debug_prefix(v)..id
end

local function debug_arg(v)
	if type(v) == 'boolean' then
		return 'Y' or 'N'
	elseif v == nil or type(v) == 'number' then
		return tostring(v)
	elseif type(v) == 'string' then
		return v
			:gsub('\r\n', '\n')
			:gsub('\n%s*$', '')
			:gsub('[%z\1-\9\11-\31\128-\255]', '.') or ''
	else --table, function, thread, cdata
		return names[v]
			or (getmetatable(v) and getmetatable(v).__tostring and tostring(v))
			or (type(v) == 'table' and not v.type and not v.debug_prefix and pp.format(v))
			or debug_id(v)
	end
end

function logging.args(...)
	if select('#', ...) == 1 then
		return debug_arg((...))
	end
	local args, n = {...}, select('#',...)
	for i=1,n do
		args[i] = debug_arg(args[i])
	end
	return unpack(args, 1, n)
end

local function log(self, severity, module, event, fmt, ...)
	if self.filter[severity] then return end
	local env1 = debug.env:upper():sub(1, 1)
	local time = time()
	local date = os.date('%Y-%m-%d %H:%M:%S', time)
	local msg = fmt and _(fmt, self.args(...))
	local entry = _('%s %s %-6s %-6s %-8s %s\n', env1, date,
		severity, module or '', event or '',
		msg and msg:gsub('\r?\n', '\n                                    ') or '')
	if severity ~= '' then
		if self.logtofile then
			self:logtofile(entry)
		end
		if self.logtoserver then
			self:logtoserver{
				env = debug.env, time = time,
				severity = severity, module = module, event = event,
				message = msg,
			}
		end
	end
	io.stderr:write(entry)
	io.stderr:flush()
end
local function note  (self, ...) log(self, 'note', ...) end
local function nolog (self, ...) log(self, '', ...) end
local function dbg   (self, ...) log(self, '', ...) end

local function warnif(self, module, event, cond, ...)
	if not cond then return end
	log(self, 'WARN', module, event, ...)
end

local function logerror(self, module, event, ...)
	log(self, 'ERROR', module, event, ...)
end

local function init(self)
	self.log      = function(...) return log      (self, ...) end
	self.note     = function(...) return note     (self, ...) end
	self.nolog    = function(...) return nolog    (self, ...) end
	self.dbg      = function(...) return dbg      (self, ...) end
	self.warnif   = function(...) return warnif   (self, ...) end
	self.logerror = function(...) return logerror (self, ...) end
	return self
end

init(logging)

logging.__index = logging

function logging.new()
	return init(setmetatable({}, logging))
end

if not ... then

	local sock = require'sock'
	sock.run(function()

		local logging = logging.new()
		logging:tofile('test.log', 64000)
		logging:toserver('127.0.0.1', 1234, 998, .5)

		for i=1,1000 do
			logging.note('test-m', 'test-ev', 'foo %d bar', i)
		end

		local sock = require'sock'
		local fs = require'fs'

		local s1 = sock.tcp()
		local s2 = sock.tcp()
		local t1 = coroutine.create(function() end)
		local t2 = coroutine.create(function() end)

		logging.dbg('test-m', 'test-ev', '%s %s %s %s\nanother thing', s1, s2, t1, t2)

	end)

end

return logging
