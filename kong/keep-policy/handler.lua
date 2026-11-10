local json = require "cjson.safe"
local policy = require "kong.plugins.keep-policy.policy"
local plugin = { PRIORITY = 1200, VERSION = "1.0.0" }
local identity_headers = {
  "x-consumer-id", "x-consumer-custom-id", "x-consumer-username",
  "x-credential-identifier", "x-keep-operator-id", "x-keep-tenant-id",
  "x-keep-policy-generation", "x-keep-routing-names",
  "x-keep-authorized-at",
}

function plugin:access(conf)
  for _, header in ipairs(identity_headers) do kong.service.request.clear_header(header) end
  if conf.probe then
    if not conf.probe_token or kong.request.get_header("x-keep-probe-token") ~= conf.probe_token then
      return kong.response.exit(401, {message = "Private proof required"})
    end
    local body = kong.request.get_body()
    if not body or type(body.operator_id) ~= "string" or
       type(body.credential_id) ~= "string" or type(body.nonce) ~= "string" then
      return kong.response.exit(400, {message = "Invalid proof"})
    end
    local result = policy.evaluate(body.operator_id, body.credential_id, conf)
    if not result then return kong.response.exit(503, {message = "Policy unavailable"}) end
    result.member_id = kong.node.get_id()
    result.nonce = body.nonce
    result.version = plugin.VERSION
    result.admitted = false
    return kong.response.exit(200, result)
  end
  local credential = kong.client.get_credential()
  local consumer = kong.client.get_consumer()
  if not credential or not consumer then return kong.response.exit(401) end
  -- Capture before reading authorization. A worker paused after an old allow
  -- result cannot resume later with a freshly timestamped authorization.
  local authorized_at = ngx.now()
  local result = policy.evaluate(consumer.id, credential.id, conf)
  if not result then return kong.response.exit(503, {message = "Policy unavailable"}) end
  if not result.allowed then return kong.response.exit(401, {message = "Credential inactive"}) end
  if conf.probe_active then
    if not conf.probe_token or kong.request.get_header("x-keep-probe-token") ~= conf.probe_token then
      return kong.response.exit(401)
    end
    result.member_id = kong.node.get_id()
    result.version = plugin.VERSION
    result.admitted = false
    return kong.response.exit(200, result)
  end
  kong.service.request.clear_header("x-api-key")
  kong.service.request.clear_header("authorization")
  kong.service.request.set_header("x-consumer-id", consumer.id)
  kong.service.request.set_header("x-credential-identifier", credential.id)
  kong.service.request.set_header("x-keep-operator-id", result.operator_id)
  kong.service.request.set_header("x-keep-tenant-id", result.tenant_id)
  kong.service.request.set_header("x-keep-policy-generation", result.generation)
  kong.service.request.set_header("x-keep-authorized-at", string.format("%.3f", authorized_at))
  kong.service.request.set_header("x-keep-routing-names", ngx.encode_base64(json.encode(result.routing_names)))
end
return plugin
