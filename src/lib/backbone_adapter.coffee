###
  backbone-sql.js 0.6.5
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-sql
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
###

{_} = require 'backbone-orm'

module.exports = class SqlBackboneAdapter
  @nativeToAttributes: (json, schema) ->
    for key, value of schema.fields
      if schema.fields[key] and schema.fields[key].type is 'Boolean' and json[key] isnt null
        json[key] = !!json[key]
      else if value.type?.toLowerCase() is 'json' and json[key] and _.isString(json[key])
        try
          json[key] = JSON.parse(json[key])
        catch err
          # console.log(err)

      # Make join table ids strings
      else if key.endsWith('_id') and json[key]
        json[key] = json[key].toString()

    # Make primary key and foreign keys strings
    json.id = json.id.toString() if json.id
    for key, relation of schema.relations
      if relation.type is 'belongsTo'
        foreign_key = relation.foreign_key
      if foreign_key and json[foreign_key]
        json[foreign_key] = json[foreign_key].toString()

    return json
