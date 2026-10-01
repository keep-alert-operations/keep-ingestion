local json = require "cjson.safe"
local _M = {}
local months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}

local function instant(value)
  if type(value) ~= "string" then return nil end
  local year, month, day, hour, minute, second = value:match("^(%d%d%d%d)%-(%d%d)%-(%d%d)T(%d%d):(%d%d):(%d%d)")
  local suffix = value:sub(20)
  if not year or not (suffix == "Z" or suffix == "+00:00" or suffix:match("^%.%d+Z$") or suffix:match("^%.%d+%+00:00$")) then return nil end
  month = months[tonumber(month)]
  if not month then return nil end
  return ngx.parse_http_time("Thu, " .. day .. " " .. month .. " " .. year .. " " .. hour .. ":" .. minute .. ":" .. second .. " GMT")
end

function _M.trusted(conf)
  if conf.unqualified_local_clock then return true end
  local file = io.open(conf.clock_health_file, "r")
  if not file then return false end
  local raw = file:read(4097)
  file:close()
  if not raw or #raw > 4096 then return false end
  local evidence = json.decode(raw)
  if type(evidence) ~= "table" or evidence.synchronized ~= true then return false end
  local offset, measured = evidence.offset_seconds, instant(evidence.measured_at)
  if type(offset) ~= "number" or offset ~= offset or math.abs(offset) > 1 or not measured then return false end
  local age = ngx.now() - measured
  return age >= -1 and age <= 60
end
return _M
