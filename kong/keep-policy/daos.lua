return {{
  name = "keep_policies",
  primary_key = { "operator_id" },
  fields = {
    { operator_id = { type = "string", uuid = true, required = true } },
    { generation = { type = "integer", required = true, between = {1, 9007199254740991} } },
    { document = { type = "string", required = true } },
  },
}}
