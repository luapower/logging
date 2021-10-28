--[[

	file and tcp logging with capped disk & memory usage.
	Written by Cosmin Apreutesei. Public domain.

	logging.log(severity, module, event, fmt, ...)
	logging.note(module, event, fmt, ...)
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
local time = require'time'
local pp = require'pp'
local glue = require'glue'

local clock = time.clock
local time = time.time
local _ = string.format

local logging = {quiet = false, verbose = true, debug = false, flush = false}

function logging:tofile(logfile, max_size)

	local fs = require'fs'

	local logfile0 = logfile:gsub('(%.[^%.]+)$', '0%1')
	if logfile0 == logfile then logfile0 = logfile..'0' end

	local f, size

	local function check(event, ret, err)
		if ret then return ret end
		self.log('', 'log', event, '%s', err)
		if f then f:close(); f = nil end
	end

	local function open()
		if f then return true end
		f = check('open', fs.open(logfile, 'a')); if not f then return end
		size = check('size', f:attr'size'); if not f then return end
		return true
	end

	local function rotate(len)
		if max_size and size + len > max_size / 2 then
			f:close(); f = nil
			if not check('move', fs.move(logfile, logfile0)) then return end
			if not open() then return end
		end
		return true
	end

	function self:logtofile(s)
		if not open() then return end
		if not rotate(#s + 1) then return end
		size = size + #s + 1
		if not check('write', f:write(s)) then return end
		if self.flush and not check('flush', f:flush()) then return end
	end

	return self
end

function logging:toserver(host, port, queue_size, timeout)

	local sock = require'sock'
	local queue = require'queue'

	local tcp

	local function check(event, ret, err)
		if ret then return ret end
		self.log('', 'log', event, '%s', err)
	end

	local function check_io(event, ret, err)
		if ret then return ret end
		check(event, ret, err)
		if tcp then tcp:close(); tcp = nil end
	end

	local function connect()
		if tcp then return tcp end
		tcp = check_io('sock.tcp', sock.tcp()); if not tcp then return end
		local exp = timeout and clock() + timeout
		if not check_io('connect', tcp:connect(host, port, exp)) then return end
		return true
	end

	local queue = queue.new(queue_size or 1/0)
	local send_thread_suspended = true
	local stop

	local send_thread = sock.newthread(function()
		send_thread_suspended = false
		local lenbuf = ffi.new'uint32_t[1]'
		while not stop do
			local msg = queue:peek()
			if msg then
				if connect() then
					local s = pp.format(msg)
					lenbuf[0] = #s
					local len = ffi.string(lenbuf, ffi.sizeof(lenbuf))
					local exp = timeout and clock() + timeout
					if check('send', tcp:send(len..s, nil, exp)) then
						queue:pop()
					end
				end
			else
				send_thread_suspended = true
				sock.suspend()
				send_thread_suspended = false
			end
		end
		check_io('stop', nil, '')
		self.logtoserver = nil
	end)

	function self:logtoserver(msg)
		if not check('push', queue:push(msg)) then
			queue:pop()
			queue:push(msg)
		end
		if send_thread_suspended then
			sock.resume(send_thread)
		end
	end

	function self:toserver_stop()
		stop = true
		if send_thread_suspended then
			sock.resume(send_thread)
		end
	end

	return self
end

function logging:toserver_stop() end

logging.filter = {}

local names = setmetatable({}, {__mode = 'k'}) --{[obj]->name}

function logging.name(obj, name)
	names[obj] = name
end

logging.name(coroutine.running(), 'TM')

local function debug_type(v)
	return type(v) == 'table' and v.type or type(v)
end

local prefixes = {
	thread = 'T',
	['function'] = 'f',
	cdata = 'c',
}

local function debug_prefix(v)
	return type(v) == 'table' and v.debug_prefix or prefixes[debug_type(v)]
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
		return v and 'Y' or 'N'
	elseif v == nil or type(v) == 'number' then
		return tostring(v)
	elseif type(v) == 'string' then
		if v:find('\n', 1, true) then --multiline
			v = v:gsub('\r\n', '\n')
			v = glue.outdent(v)
			v = '\n\n'..v..'\n'
		end
		v = v:gsub('[%z\1-\8\11-\31\128-\255]', '.')
		return v
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
	local env = debug.env and debug.env:upper():sub(1, 1) or 'D'
	local time = time()
	local date = os.date('%Y-%m-%d %H:%M:%S', time)
	local msg = fmt and _(fmt, self.args(...))
	if msg and msg:find('\n', 1, true) then --multiline
		local arg1_multiline = msg:find'^\n\n'
		msg = glue.outdent(msg, '\t')
		if not arg1_multiline then
			msg = '\n\n'..msg..'\n'
		end
	end
	local entry = _('%s %s %-6s %-6s %-8s %-4s %s\n',
		env, date, severity, module or '', (event or ''):sub(1, 8),
		debug_arg(coroutine.running()), msg or '')
	if severity ~= '' then --debug messages are transient
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
	if
		not self.quiet
		and (severity ~= '' or self.debug)
		and (severity ~= 'note' or (self.verbose == true or self.verbose == module))
	then
		io.stderr:write(entry)
		io.stderr:flush()
	end
end
local function note (self, ...) log(self, 'note', ...) end
local function dbg  (self, ...) log(self, '', ...) end

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

	local logging = logging.new()

	sock.thread(function()
		sock.sleep(5)
		logging:toserver_stop()
		print'told to stop'
	end)

	sock.run(function()

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
