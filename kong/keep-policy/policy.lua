local json = require "cjson.safe"
local _M = {}
local sha256 = require "resty.sha256"
local hex = require "resty.string".to_hex
local clock = require "kong.plugins.keep-policy.clock"
function _M.digest(value)
  local hash = sha256:new()
  hash:update(value)
  return hex(hash:final())
end

function _M.evaluate(operator, credential, conf)
  -- Read the Kong policy authority on each request. No node cache can extend a
  -- revoked credential's lifetime; a policy-store outage fails closed.
  local row, err = kong.db.keep_policies:select({ operator_id = operator })
  if err or not row then return nil, "policy_unavailable" end
  local doc = json.decode(row.document)
  if not doc or doc.operator_id ~= operator then return nil, "policy_invalid" end
  local key = doc.credentials and doc.credentials[credential]
  if doc.enabled == true and key and (key.state == "current" or key.state == "retiring") and not clock.trusted(conf) then
    return nil, "clock_untrusted"
  end
  local allowed = doc.enabled == true and key and
    (key.state == "current" or key.state == "retiring") and
    (key.state ~= "retiring" or type(key.expires_at) == "number") and
    (not key.expires_at or type(key.expires_at) == "number") and
    (not key.expires_at or ngx.now() < key.expires_at)
  return {
    allowed = allowed == true, generation = tostring(row.generation),
    operator_id = operator, tenant_id = doc.tenant_id,
    credential_id = credential, routing_names = doc.routing_names,
    policy_digest = _M.digest(row.document), expires_at = key and key.expires_at,
    state = key and key.state or "inactive",
  }
end
return _M
