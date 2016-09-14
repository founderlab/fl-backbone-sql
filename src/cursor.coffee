###
  backbone-sql.js 0.6.5
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-sql
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
###

Knex = require 'knex'
{_, sync} = require 'backbone-orm'
Ast = require './ast'

COMPARATORS =
  $lt: '<'
  $lte: '<='
  $gt: '>'
  $gte: '>='
  $ne: '!='
COMPARATOR_KEYS = _.keys(COMPARATORS)

_extractCount = (count_json) ->
  return 0 unless count_json?.length
  count_info = count_json[0]
  return +(count_info[if count_info.hasOwnProperty('count(*)') then 'count(*)' else 'count'])

_appendCondition = (conditions, key, value, method='where') ->
  if value?.$in
    if value.$in?.length then conditions.wheres.push({key, method: 'whereIn', value: value.$in}) else (conditions.abort = true; return conditions)

  else if value?.$nin
    if value.$nin?.length then conditions.wheres.push({key, method: 'whereNotIn', value: value.$nin})

  else if value?.$exists?
    conditions.wheres.push({method: (if value?.$exists then 'whereNotNull' else 'whereNull'), key: key})

  # Transform a conditional of type {key: {$lt: 5}} to ('key', '<', 5)
  else if _.isObject(value) and ops_length = _.size(mongo_ops = _.pick(value, COMPARATOR_KEYS))
    operations = []
    for mongo_op, parameter of mongo_ops
      # TODO: should return an error for null on an operator unless it is $ne, but there is no callback
      throw new Error "Unexpected null with query key '#{key}' operator '#{operator}'" if _.isNull(value) and (operator isnt '$ne')
      operations.push({operator: COMPARATORS[mongo_op], value: parameter})
    if ops_length is 1
      conditions.where_conditionals.push(_.extend(operations[0], {key}))
    else
      conditions.where_conditionals.push({key, operations})

  # Transform a conditional of type {key: {$like: 'string'}} to ('key', 'like', '%string%')
  else if _.isObject(value) and value.$like
    test_str = if '%' in value.$like then value.$like else "%#{value.$like}%"
    conditions.where_conditionals.push({key, method, operator: 'ilike', value: test_str})

  else if method is 'where'
    method = 'whereNull' if _.isNull(value)
    conditions.wheres.push({key, value, method})

  else if method is 'orWhere'
    method = 'orWhereNull' if _.isNull(value)
    console.log('OR', {key, value, method})
    conditions.or_wheres.push({key, value, method})

  return conditions

_columnName = (col, table) -> if table then "#{table}.#{col}" else col

_appendConditionalWhere = (query, key, condition, table, compound) ->
  whereMethod = if compound then 'andWhere' else 'where'
  if condition.operator is '!='
    # != should function like $ne, including nulls
    query[whereMethod] ->
      if _.isNull(condition.value)
        @whereNotNull(_columnName(key, table))
      else
        @where(_columnName(key, table), condition.operator, condition.value).orWhereNull(_columnName(key, table))
  else
    query[whereMethod](_columnName(key, table), condition.operator, condition.value)

_appendWhere = (query, conditions, table) ->
  for condition in conditions.wheres or []
    query[condition.method](_columnName(condition.key, table), condition.value)

  # Bundle the or wheres up together as they'll come from one $or prop
  # SQL should look like `where (prop1 = val1 or prop2 = val2)`
  # console.log('conditions1', conditions.or_wheres, conditions)
  if conditions.or_wheres.length
    query.where ->
      nested_query = @
      for condition in conditions.or_wheres
        # console.log('condition.method inner', condition.method)
        nested_query[condition.method](_columnName(condition.key, table), condition.value)

  # Handling `where something > someValue`
  for condition in conditions.where_conditionals
    if condition.operations
      do (condition) -> query.where ->
        operation = condition.operations.pop()
        nested_query = @
        _appendConditionalWhere(nested_query, condition.key, operation, table, false)
        for operation in condition.operations
          _appendConditionalWhere(nested_query, condition.key, operation, table, true)

    else if _.isNull(condition.value)
      query.whereNotNull(_columnName(condition.key, table))

    else
      _appendConditionalWhere(query, condition.key, condition, table, false)

  return query












###
New
###


# TODO: look at optimizing without left outer joins everywhere
# Make another query to get the complete set of related objects when they have been fitered by a where clause
_joinToRelation = (query, model_type, relation, options={}) ->
  related_model_type = relation.reverse_relation.model_type

  if relation.type is 'hasMany' and relation.reverse_relation.type is 'hasMany'
    pivot_table = relation.join_table.tableName()

    # Join the from model to the pivot table
    from_key = "#{model_type.tableName()}.id"
    pivot_to_key = "#{pivot_table}.#{relation.foreign_key}"
    query.join(pivot_table, from_key, '=', pivot_to_key, 'left outer')

    unless options.pivot_only
      # Then to the to model's table (only if we need data from them second table)
      pivot_from_key = "#{pivot_table}.#{relation.reverse_relation.foreign_key}"
      to_key = "#{related_model_type.tableName()}.id"
      query.join(related_model_type.tableName(), pivot_from_key, '=', to_key, 'left outer')

  else
    if relation.type is 'belongsTo'
      from_key = "#{model_type.tableName()}.#{relation.foreign_key}"
      to_key = "#{related_model_type.tableName()}.id"
    else
      from_key = "#{model_type.tableName()}.id"
      to_key = "#{related_model_type.tableName()}.#{relation.foreign_key}"
    query.join(related_model_type.tableName(), from_key, '=', to_key, 'left outer')

_appendWhereAst = (query, condition) ->
  # console.log('Building', condition)
  if !_.isUndefined(condition.key)
    if condition.operator
      query[condition.method](condition.key, condition.operator, condition.value)
    else
      query[condition.method](condition.key, condition.value)

  else if condition.conditions?.length
    query[condition.method] ->
      sub_query = @
      for c in condition.conditions
        _appendWhereAst(sub_query, c)

  # console.log('query', query.toString())
  return query

_parseSortField = (sort) ->
  if sort[0] is '-'
    dir = 'desc'
    col = sort.substr(1)
  else
    dir = 'asc'
    col = sort
  return [col, dir]

_appendSelect = (query, ast) ->
  query.select(ast.select)
  return query

_appendSort = (query, sort_fields) ->
  return query unless sort_fields
  for sort in sort_fields
    [col, dir] = _parseSortField(sort)
    query.orderBy(col, dir)
  return query

_appendLimits = (query, limit, offset) ->
  query.limit(limit) if limit
  query.offset(offset) if offset
  return query

buildQueryFromAst = (query, ast, options={}) ->
  _appendWhereAst(query, ast.where)

  if (_.size(ast.joins))
    for key, join of ast.joins
      join_options = {pivot_only: join.pivot_only and not (join.include or join.condition)}
      _joinToRelation(query, ast.model_type, join.relation, join_options)
  else
    _appendLimits(query, ast.limit, ast.offset)

  return query.count('*') if ast.count or options.count
  return query.count('*').limit(1) if ast.exists or options.exists

  _appendSelect(query, ast)
  _appendSort(query, ast.sort)

  return query





  # Rows returned from a join query need to be un-merged into the correct json format
_unjoinResults = (raw_json, ast, parseJson) ->
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
          if match = ast.prefixRegex(join.model_type.tableName()).exec(key)
            related_json[match[1]] = value

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
        reverse_relation_schema = model_type.relation(relation_key).reverse_relation.model_type.schema()
        related_json = parseJson(related_json, reverse_relation_schema)
        # related_json = @backbone_adapter.nativeToAttributes(related_json, reverse_relation_schema)
        if model_type.relation(relation_key).type is 'hasMany'
          model_json[relation_key] or= []
          model_json[relation_key].push(related_json) unless _.find(model_json[relation_key], (test) -> test.id is related_json.id)
        else
          model_json[relation_key] = related_json

  return json





module.exports = class SqlCursor extends sync.Cursor
  verbose: false
  # verbose: true

  _parseConditions: (find, cursor, conditions={wheres: [], or_wheres: [], where_conditionals: [], related_wheres: {}, joined_wheres: {}}, method='where') ->
    related_wheres = {}

    for key, value of find
      throw new Error "Unexpected undefined for query key '#{key}'" if _.isUndefined(value)

      # A dot indicates a condition on a related model
      if key.indexOf('.') > 0
        [relation, key] = key.split('.')
        related_wheres[relation] or= {}
        related_wheres[relation][key] = value

      # Many to Many relationships may be queried on the foreign key of the join table
      else if (reverse_relation = @model_type.reverseRelation(key)) and reverse_relation.join_table
        console.dir("'**************#{key}****************'", {colors: true})
        relation = reverse_relation.reverse_relation
        console.log('relation.key', relation.key)
        conditions.joined_wheres[relation.key] or= {wheres: [], or_wheres: [], where_conditionals: []}
        _appendCondition(conditions.joined_wheres[relation.key], key, value, method)
      else
        _appendCondition(conditions, key, value, method)

    # Parse conditions on related models in the same way
    conditions.related_wheres[relation] = @_parseConditions(related_conditions) for relation, related_conditions of related_wheres

    if cursor?.$or
      for cond in cursor.$or
        @_parseConditions(cond, {}, conditions, 'orWhere')

    if cursor?.$ids
      (conditions.abort = true; return conditions) unless cursor.$ids.length
      conditions.wheres.push({method: 'whereIn', key: 'id', value: cursor.$ids})

    return conditions

  execUnique: (callback) ->
    try
      #TEMP
      @_conditions = @_parseConditions(@_find, @_cursor)


      ast = new Ast({
        find: @_find,
        cursor: @_cursor,
        model_type: @model_type,
        prefix_columns: false,
      })
      query = @connection(@model_type.tableName())
      query = buildQueryFromAst(query, ast)

      console.log('========================query=========================')

      if @_cursor.$count
        query.count().from(@connection.distinct(@_cursor.$unique).from(@model_type.tableName()).as('count_query'))
        return query.exec (err, count_json) => callback(err, _extractCount(count_json))

      # We're not selecting any fields outside of those in $unique, so we can use distinct
      if _.difference(ast.select, @_cursor.$unique).length is 0
        query.distinct(ast.select)

      # Other fields are required - uses partition, a postgres window function
      else
        rank_field = @_cursor.$unique[0]
        [sort_field, sort_dir] = @_parseSortField(ast.sort?[0] or 'id')
        subquery = @connection.select(@connection.raw("#{ast.select.join(', ')}, rank() over (partition by #{rank_field} order by #{sort_field} #{sort_dir})"))
        subquery.from(@model_type.tableName()).as('subquery')
        query.select(ast.select).from(subquery).where('rank', 1)

      console.dir(query.toString(), {depth: null, colors: true})

      return @_exec query, ast, callback

    catch err
      return callback(new Error("Query failed for model: #{@model_type.model_name} with error: #{err}"))

  queryToJSON: (callback) ->
    return callback(null, if @hasCursorQuery('$one') then null else []) if @hasCursorQuery('$zero')

    # Unique
    return @execUnique(callback) if @_cursor.$unique

    try
      @_conditions = @_parseConditions(@_find, @_cursor)
      # $in : [] or another query that would result in an empty result set in mongo has been given
      return callback(null, if @_cursor.$count then 0 else (if @_cursor.$one then null else [])) if @_conditions.abort

      # Special cases: $zero, $count, $exists, $unique?
      ###
      ###
      console.log()
      console.log()
      console.log()
      ast = new Ast({
        find: @_find,
        cursor: @_cursor,
        model_type: @model_type,
      })
      ast.print()
      console.log('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
      console.dir(@_conditions, {depth: null, colors: true})
      console.log('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')

      console.log()
      console.log()

      query = @connection(@model_type.tableName())
      query = buildQueryFromAst(query, ast)
      @joined = true

    catch err
      return callback("Query failed for model: #{@model_type.model_name} with error: #{err}")


    # count and exists when there is not a join table
    if @_cursor.$count or @_cursor.$exists
      # @_appendRelatedWheres(query)
      # @_appendJoinedWheres(query)

      return query.exec (err, count_json) =>
        return callback(err) if (err)
        count = _extractCount(count_json)
        callback(null, if @_cursor.$count then count else (count > 0))

    # if @_cursor.$include
    #   @include_keys = if _.isArray(@_cursor.$include) then @_cursor.$include else [@_cursor.$include]
    #   throw Error("Invalid include specified: #{@include_keys}") unless @include_keys.length

    #   # Join the related tables
    #   @joined = true
    #   to_columns = []
    #   for key in @include_keys
    #     relation = @_getRelation(key)
    #     related_model_type = relation.reverse_relation.model_type

    #     # Compile the columns for the related model and prefix them with its table name
    #     to_columns = to_columns.concat(@_prefixColumns(related_model_type))

    #     @_joinTo(query, relation)

    #     # Use the full table name when adding the where clauses
    #     if related_wheres = @_conditions.related_wheres[key]
    #       (@queued_queries or= []).push(key)
    #       _appendWhere(query, related_wheres, related_model_type.tableName())

    #   # Compile the columns for this model and prefix them with its table name
    #   from_columns = @_prefixColumns(@model_type, ast.fields)
    #   $columns = from_columns.concat(to_columns)

    # else
      # TODO: do these make sense with joins? apply them after un-joining the result?
      # @_appendLimits(query)


    if true
      console.log('=======================query_old=======================')
      query_old = @connection(@model_type.tableName())
      _appendWhere(query_old, @_conditions, @model_type.tableName())

      $columns = if @joined then @_prefixColumns(@model_type, ast.fields) else @_columns(@model_type, ast.fields)
      query_old.select($columns)
      # @_appendSort(query_old)

      # Append where conditions and join if needed for the form `related_model.field = value`
      @_appendRelatedWheres(query_old)

      # Append where conditions and join if needed for the form `manytomanyrelation_id.field = value`
      @_appendJoinedWheres(query_old)

      console.dir(query_old.toString(), {depth: null, colors: true})
      console.log('=======================================================')

      console.log()
      console.log()

    console.log('========================query=========================')
    console.dir(query.toString(), {depth: null, colors: true})
    @_exec query, ast, callback

  _exec: (query, ast, callback) =>
    if @verbose
      console.log '\n----------'
      console.dir(query.toString(), {depth: null, colors: true})
      console.log '----------'
    query.exec (err, json) =>

      console.log('PRE UNJOIN JSON:')
      console.dir(json, {depth: null, colors: true})

      return callback(new Error("Query failed for model: #{@model_type.model_name} with error: #{err}")) if err
      json = _unjoinResults(json, ast, @backbone_adapter.nativeToAttributes) if ast.prefix_columns

      console.log('UNJOINED JSON:')
      console.dir(json, {depth: null, colors: true})

      console.log('joinedIncludesWithConditions', ast.joinedIncludesWithConditions())

      if ast.joinedIncludesWithConditions().length
        @_appendCompleteRelations(json, ast, callback)
      else
        @_processResponse(json, ast, callback)

  # Process any remaining queries and return the json
  _processResponse: (json, ast, callback) ->
    schema = @model_type.schema()

    @backbone_adapter.nativeToAttributes(model_json, schema) for model_json in json
    json = @selectResults(json)

    # NOTE: limit and offset would apply to the join table so do as post-process. TODO: optimize
    if @_cursor.$include
      console.log(@_cursor)
      if @_cursor.$offset
        number = json.length - @_cursor.$offset
        number = 0 if number < 0
        json = if number then json.slice(@_cursor.$offset, @_cursor.$offset+number) else []

      if @_cursor.$limit
        json = json.splice(0, Math.min(json.length, @_cursor.$limit))

    if @_cursor.$page
      query = @connection()
      query = buildQueryFromAst(query, ast, {count: true})

      query_old = @connection()
      _appendWhere(query_old, @_conditions, @model_type.tableName())
      @_appendRelatedWheres(query_old)
      @_appendJoinedWheres(query_old)
      query_old.count('*')

      console.log('--------NEW---------')
      console.dir(query.toString(), {colors: true})
      console.log('-----------------')

      console.log('--------OLD---------')
      console.dir(query_old.toString(), {colors: true})
      console.log('-----------------')

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

      query.exec (err, count_json) =>
        return callback(err) if err
        console.log('\nFINAL (count):', {
          offset: @_cursor.$offset or 0
          total_rows: _extractCount(count_json)
          rows: json
        })
        callback(null, {
          offset: @_cursor.$offset or 0
          total_rows: _extractCount(count_json)
          rows: json
        })
    else
      console.log('\nnFINAL JSON:')
      console.dir(json, {depth: null, colors: true})

      callback(null, json)

  # Make another query to get the complete set of related objects when they have been fitered by a where clause
  _appendCompleteRelations: (json, ast, callback) ->
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

    relation_query.exec (err, raw_relation_json) =>
      return callback(err) if err
      relation_json = _unjoinResults(raw_relation_json, relation_ast, @backbone_adapter.nativeToAttributes)
      for placeholder in relation_json
        model = _.find(json, (test) -> test.id is placeholder.id)
        _.extend(model, placeholder)
      @_processResponse(json, ast, callback)




  _appendRelatedWheres: (query) ->
    return if _.isEmpty(@_conditions.related_wheres)

    @joined = true
    # Skip any relations we've processed with $include
    if @include_keys
      @_conditions.related_wheres = _.omit(@_conditions.related_wheres, @include_keys)

    # Join the related table and add the related where conditions, using the full table name, for each related query
    for key, related_wheres of @_conditions.related_wheres
      relation = @_getRelation(key)
      @_joinTo(query, relation)
      _appendWhere(query, related_wheres, relation.reverse_relation.model_type.tableName())

  _appendJoinedWheres: (query) ->
    return if _.isEmpty(@_conditions.joined_wheres)

    @joined = true
    # Ensure that a join with the join table occurs and add the where clause for the foreign key
    for key, joined_wheres of @_conditions.joined_wheres
      relation = @_getRelation(key)
      console.log('_appendJoinedWheres', key)
      unless key in _.keys(@_conditions.related_wheres) or (@include_keys and key in @include_keys)
        from_key = "#{@model_type.tableName()}.id"
        to_key = "#{relation.join_table.tableName()}.#{relation.foreign_key}"
        query.join(relation.join_table.tableName(), from_key, '=', to_key, 'left outer')
      _appendWhere(query, joined_wheres, relation.join_table.tableName())

  _columns: (model_type, fields) ->
    columns = if fields then _.clone(fields) else model_type.schema().columns()
    columns.push('id') unless 'id' in columns
    return columns

  _prefixColumns: (model_type, fields) ->
    columns = if fields then _.clone(fields) else model_type.schema().columns()
    columns.push('id') unless 'id' in columns
    return ("#{model_type.tableName()}.#{col} as #{@_tablePrefix(model_type)}#{col}" for col in columns)

  _tablePrefix: (model_type) -> "#{model_type.tableName()}_"

  _prefixRegex: (model_type) -> new RegExp("^#{@_tablePrefix(model_type)}(.*)$")

  _getRelation: (key) ->
    throw new Error("#{key} is not a relation of #{@model_type.model_name}") unless relation = @model_type.relation(key)
    return relation

  # TODO: look at optimizing without left outer joins everywhere
  # Make another query to get the complete set of related objects when they have been fitered by a where clause
  _joinTo: (query, relation) ->
    related_model_type = relation.reverse_relation.model_type
    if relation.type is 'hasMany' and relation.reverse_relation.type is 'hasMany'
      pivot_table = relation.join_table.tableName()

      # Join the from model to the pivot table
      from_key = "#{@model_type.tableName()}.id"
      pivot_to_key = "#{pivot_table}.#{relation.foreign_key}"
      query.join(pivot_table, from_key, '=', pivot_to_key, 'left outer')

      # Then to the to model's table
      pivot_from_key = "#{pivot_table}.#{relation.reverse_relation.foreign_key}"
      to_key = "#{related_model_type.tableName()}.id"
      query.join(related_model_type.tableName(), pivot_from_key, '=', to_key, 'left outer')
    else
      if relation.type is 'belongsTo'
        from_key = "#{@model_type.tableName()}.#{relation.foreign_key}"
        to_key = "#{related_model_type.tableName()}.id"
      else
        from_key = "#{@model_type.tableName()}.id"
        to_key = "#{related_model_type.tableName()}.#{relation.foreign_key}"
      query.join(related_model_type.tableName(), from_key, '=', to_key, 'left outer')
