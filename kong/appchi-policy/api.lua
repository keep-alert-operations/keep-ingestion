local json = require "cjson.safe"
return {
  ["/appchi-policy-node"] = { GET = function()
    return kong.response.exit(200, {member_id = kong.node.get_id(), version = "1.0.0"})
  end },
  ["/appchi-policies/:operator_id"] = {
    PUT = function(self, db)
      local doc = self.params.document
      local decoded = type(doc) == "string" and json.decode(doc)
      local generation = tonumber(self.params.generation)
      if not decoded or decoded.operator_id ~= self.params.operator_id or
         not generation or generation % 1 ~= 0 or generation < 1 or
         generation > 9007199254740991 then
        return kong.response.exit(400, {message = "Invalid policy"})
      end
      local row, err = kong.db.appchi_policies:upsert({operator_id = self.params.operator_id},
        {operator_id = self.params.operator_id, generation = generation, document = doc})
      if err then return kong.response.exit(409, {message = "Policy generation conflict"}) end
      return kong.response.exit(200, row)
    end,
    GET = function(self, db)
      local row, err = kong.db.appchi_policies:select({operator_id = self.params.operator_id})
      if err then return kong.response.exit(503, {message = "Policy unavailable"}) end
      return kong.response.exit(row and 200 or 404, row)
    end,
  },
}
