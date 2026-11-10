local typedefs = require "kong.db.schema.typedefs"
return {
  name = "keep-policy",
  fields = {
    { consumer = typedefs.no_consumer },
    { protocols = typedefs.protocols_http },
    { config = { type = "record", fields = {
      { probe = { type = "boolean", default = false } },
      { probe_active = { type = "boolean", default = false } },
      { probe_token = { type = "string" } },
      { member_id = { type = "string", required = true } },
      { clock_health_file = { type = "string", default = "/run/keep/clock-health.json" } },
      { unqualified_local_clock = { type = "boolean", default = false } },
    } } },
  },
}
