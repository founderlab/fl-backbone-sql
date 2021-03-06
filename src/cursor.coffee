###
  backbone-sql.js 0.6.5
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-sql
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
###

Knex = require 'knex'
{_, sync} = require 'backbone-orm'
Ast = require './ast'
buildQueryFromAst = require './query_builder'
Utils = require './lib/utils'

extractCount = (count_json) ->
  return 0 unless count_json?.length
  count_info = count_json[0]
  return +(count_info[if count_info.hasOwnProperty('count(*)') then 'count(*)' else 'count'])

module.exports = class SqlCursor extends sync.Cursor
  verbose: false
  # verbose: true

  execUnique: (callback) =>
    try
      ast = new Ast({
        find: @_find,
        cursor: @_cursor,
        model_type: @model_type,
        prefix_columns: false,
      })
      query = @connection(@model_type.tableName())
      query = buildQueryFromAst(query, ast, {skipSelect: true})

      if @_cursor.$count
        query.count().from(@connection.distinct(@_cursor.$unique).from(@model_type.tableName()).as('count_query'))
        return query.asCallback (err, count_json) => callback(err, extractCount(count_json))

      # We're not selecting any fields outside of those in $unique, so we can use distinct
      if _.difference(ast.select, @_cursor.$unique).length is 0
        query.distinct(ast.select)

      # Other fields are required - uses partition, a postgres window function
      else
        rank_field = @_cursor.$unique[0]
        raw_query = "#{ast.select.map((s) => "\"#{s}\"").join(', ')}, rank() over (partition by \"#{rank_field}\""
        if ast.sort?.length
          if sort = ast.sort?.shift()
            raw_query += " order by \"#{sort.column}\" #{sort.direction}"
          for sort in ast.sort?
            raw_query += " , \"#{sort.column}\" #{sort.direction}"
        raw_query += ")"
        subquery = @connection.select(@connection.raw(raw_query))
        subquery.from(@model_type.tableName()).as('subquery')
        console.log('subquery', subquery.toString())
        console.log('ast.select', ast.select)
        query.select(ast.select).from(subquery).where('rank', 1)
        console.log('query', query.toString())

      return @runQuery query, ast, callback

    catch err
      return callback(new Error("Query failed for model: #{@model_type.model_name} with error: #{err}"))

  queryToJSON: (callback) =>
    return callback(null, if @hasCursorQuery('$one') then null else []) if @hasCursorQuery('$zero')

    @verbose or= @_cursor.$verbose
    if @verbose
      @start_time = new Date().getTime()

    @_cursor.$count = true if @hasCursorQuery('$count')
    @_cursor.$exists = true if @hasCursorQuery('$exists')

    # Unique
    return @execUnique(callback) if @_cursor.$unique

    try
      ast = new Ast({
        find: @_find,
        cursor: @_cursor,
        model_type: @model_type,
      })
      query = @connection(@model_type.tableName())
      query = buildQueryFromAst(query, ast)

      # $in : [] or another query that would result in an empty result set in mongo has been given
      return callback(null, if @_cursor.$count then 0 else (if @_cursor.$one then null else [])) if ast.abort

    catch err
      return callback("Query failed for model: #{@model_type.model_name} with error: #{err}")

    @runQuery query, ast, callback

  runQuery: (query, ast, _callback) =>
    callback = _callback
    if @verbose
      @query_ready_time = new Date().getTime()

      console.log '\n----------'
      ast.print()
      console.dir(query.toString(), {depth: null, colors: true})
      console.log('Built in', @query_ready_time-@start_time, 'ms')
      console.log '----------'
      callback = (err, res) =>
        console.log('Query complete in', new Date().getTime() - @start_time, 'ms')
        _callback(err, res)

    query.asCallback (err, json) =>
      return callback(new Error("Query failed for model: #{@model_type.model_name} with error: #{err}")) if err

      if @hasCursorQuery('$count') or @hasCursorQuery('$exists')
        count = extractCount(json)
        return callback(null, if @hasCursorQuery('$count') then count else (count > 0))

      json = @unjoinResults(json, ast) if ast.prefix_columns

      if ast.joinedIncludesWithConditions().length
        @fetchIncludedRelations(json, ast, callback)
      else
        @processResponse(json, ast, callback)

  # Process any remaining queries and return the json
  processResponse: (json, ast, callback) =>
    schema = @model_type.schema()

    @backbone_adapter.nativeToAttributes(model_json, schema) for model_json in json
    json = @selectResults(json)

    # NOTE: limit and offset would apply to the join table so do as post-process. TODO: optimize
    if @_cursor.$include
      if @_cursor.$offset
        number = json.length - @_cursor.$offset
        number = 0 if number < 0
        json = if number then json.slice(@_cursor.$offset, @_cursor.$offset+number) else []

      if @_cursor.$limit
        json = json.splice(0, Math.min(json.length, @_cursor.$limit))

    if @hasCursorQuery('$page')
      query = @connection(@model_type.tableName())
      query = buildQueryFromAst(query, ast, {count: true})

      if @_cursor.$unique
        subquery = @connection.distinct(@_cursor.$unique)
        subquery.from(@model_type.tableName()).as('subquery')
        query.from(subquery)
      else
        query.from(@model_type.tableName())

      if @verbose
        console.log '\n---------- counting rows for $page ----------'
        console.dir query.toString(), {colors: true}
        console.log '---------------------------------------------'

      query.asCallback (err, count_json) =>
        return callback(err) if err
        callback(null, {
          offset: @_cursor.$offset or 0
          total_rows: extractCount(count_json)
          rows: json
        })
    else
      callback(null, json)

  # Make another query to get the complete set of related objects when they have been fitered by a where clause
  fetchIncludedRelations: (json, ast, callback) =>
    relation_ast = new Ast({
      model_type: @model_type
      query: {
        id: {$in: _.pluck(json, 'id')}
        $select: ['id']
        $include: (j.key for j in ast.joinedIncludesWithConditions())
      }
    })
    relation_query = @connection(@model_type.tableName())
    relation_query = buildQueryFromAst(relation_query, relation_ast)

    relation_query.asCallback (err, raw_relation_json) =>
      return callback(err) if err
      relation_json = @unjoinResults(raw_relation_json, relation_ast)
      for placeholder in relation_json
        model = _.find(json, (test) -> test.id is placeholder.id)
        _.extend(model, placeholder)
      @processResponse(json, ast, callback)

  # Rows returned from a join query need to be un-merged into the correct json format
  unjoinResults: (raw_json, ast) =>
    return raw_json unless raw_json and raw_json.length
    json = []
    model_type = ast.model_type

    for row in raw_json
      model_json = {}
      row_relation_json = {}

      # Fields are prefixed with the table name of the model they belong to so we can test which the values are for
      # and assign them to the correct object
      for key, value of row
        if match = ast.prefixRegex().exec(key)
          model_json[match[1]] = value

        else
          for relation_key, join of ast.joins when join.include
            related_json = (row_relation_json[relation_key] or= {})
            if match = ast.prefixRegex(join.relation.reverse_model_type.tableName()).exec(key)
              related_json[match[1]] = value
              found = true
          unless found
            model_json[key] = value

      # If there was a hasMany relationship or multiple $includes we'll have multiple rows for each model
      if found = _.find(json, (test) -> test.id is model_json.id)
        model_json = found
      # Add this model to the result if we haven't already
      else
        json.push(model_json)

      # Add relations to the model_json if included
      for relation_key, related_json of row_relation_json

        if _.isNull(related_json.id)
          if model_type.relation(relation_key).type is 'hasMany'
            model_json[relation_key] = []
          else
            model_json[relation_key] = null

        else unless _.isEmpty(related_json)
          reverse_relation_schema = model_type.relation(relation_key).reverse_model_type.schema()
          related_json = @backbone_adapter.nativeToAttributes(related_json, reverse_relation_schema)

          if model_type.relation(relation_key).type is 'hasMany'
            model_json[relation_key] or= []
            model_json[relation_key].push(related_json) unless _.find(model_json[relation_key], (test) -> test.id is related_json.id)
          else
            model_json[relation_key] = related_json

    return json

  selectResults: (json) ->
    json = json.slice(0, 1) if @_cursor.$one

    # TODO: OPTIMIZE TO REMOVE 'id' and '_rev' if needed
    if @_cursor.$values
      $values = if @_cursor.$whitelist then _.intersection(@_cursor.$values, @_cursor.$whitelist) else @_cursor.$values
      if @_cursor.$values.length is 1
        key = @_cursor.$values[0]
        json = if $values.length then ((if item.hasOwnProperty(key) then item[key] else null) for item in json) else _.map(json, -> null)
      else
        json = (((item[key] for key in $values when item.hasOwnProperty(key))) for item in json)

    else if @_cursor.$select
      $select = ((if '.' in field then field.split('.').pop() else field) for field in @_cursor.$select)
      $select = _.intersection($select, @_cursor.$whitelist) if @_cursor.$whitelist
      json = (_.pick(item, $select) for item in json)

    else if @_cursor.$whitelist
      json = (_.pick(item, @_cursor.$whitelist) for item in json)

    return json if @hasCursorQuery('$page') # paging expects an array
    return if @_cursor.$one then (json[0] or null) else json
