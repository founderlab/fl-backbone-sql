###
  backbone-sql.js 0.6.5
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-sql
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
###
_ = require 'lodash'

COMPARATORS =
  $lt: '<'
  $lte: '<='
  $gt: '>'
  $gte: '>='
  $ne: '!='
  $eq: '='
COMPARATOR_KEYS = _.keys(COMPARATORS)


###
tree = {

  select: ['t.id', 't.name'],

  where: {
    method: 'where',
    conditions: [{
      method: 'where',
      column: 't.id',
      value: 1,
    }, {
      method: 'where',
      operator: '>',
      column: 't.id',
      value: 2,
    }, {
      method: 'where',
      conditions: [
        {method: 'orWhere', column: 't.id', value: 2},
        {method: 'orWhere', column: 't.id', value: 3},
        {method: 'orWhere', column: 't.id', operator: '>=' value: 99},
      ],
    }],
  },

  join: {

  },

  sort: ['t.id'],

  limit: 5,

}
###

#   unique: {

#   },
# [{
#   method: 'where'
# },
# {method: 'join',}
# ]

# Make conditions flat list of condition objects {mehod, column, operator, value}
# Each condition can contain a conditions array
# Each item in nested conditions array has its own field key
# Recurse to add nested conditions
# Each condition starts its own where block if it had subconditions
# $or has a conditions array with potentially different keys on each condition

module.exports = class SqlAst

  constructor: (options) ->
    @select = []
    @where = {
      method: 'where'
      conditions: []
    }
    @joins = {}
    @sort = null
    @limit = null
    @parse(options) if options

  print: ->
    console.log('********************** AST ******************************')

    console.log('---- Input ----')
    console.log('> query:', @query)

    console.log()

    console.log('----  AST  ----')
    console.log('> select:', @select)
    console.log('> where:')
    console.dir(@where, {depth: null, colors: true})
    console.log('> joins:', ([key, join.columns] for key, join of @joins))
    console.log('> count:', @count)
    console.log('> exists:', @exists)
    console.log('> sort:', @sort)
    console.log('> limit:', @limit)

    console.log('---------------------------------------------------------')

  # Public method that sets up for parsing
  parse: (options) ->
    @find = options.find || {}
    @cursor = options.cursor || {}
    @query = options.query or _.extend({}, @find, @cursor)
    throw new Error('Ast requires a model_type option') unless @model_type = options.model_type

    @prefix_columns = options.prefix_columns isnt false

    if @query.$sort
      @sort = if _.isArray(@query.$sort) then @query.$sort else [@query.$sort]

    @count = true if @query.$count
    @exists = true if @query.$exists
    @limit = @query.$limit or (if @query.$one then 1 else null)
    @offset = @query.$offset

    if @query.$include
      @prefix_columns = true
      @join(key, @getRelation(key), {include: true}) for key in @query.$include
      console.log('joined', ([k, v.columns] for k, v of @joins))
    @_parse(@query, {table: @model_type.tableName()})

    @setSelectedColumns()

  columnName: (col, table) -> if table and @prefix_columns then "#{table}.#{col}" else col

  prefixColumn: (col, table) -> "#{table}.#{col} as #{@tablePrefix(table)}#{col}"

  prefixColumns: (cols, table) -> @prefixColumn(col, table) for col in cols

  tablePrefix: (table) -> "#{table}_"

  prefixRegex: (table) ->
    table or= @model_type.tableName()
    new RegExp("^#{@tablePrefix(table)}(.*)$")

  getRelation: (key, model_type) ->
    model_type or= @model_type
    throw new Error("#{key} is not a relation of #{model_type.model_name}") unless relation = model_type.relation(key)
    return relation

  join: (key, relation, options={}) ->
    console.log('JOINING', key)
    relation or= @getRelation(key)
    model_type = relation.reverse_relation.model_type
    @joins[key] = _.extend((@joins[key] or {}), {
      key
      relation
      model_type
      columns: @prefixColumn(col, model_type.tableName()) for col in model_type.schema().columns()
    }, options)

  # joinedIncludesWithConditions: -> _(@joins).map((j, k) -> if j.include and j.condition then k else null).compact().value()
  joinedIncludesWithConditions: -> join for key, join of @joins when (join.include and join.condition)

  # Internal parse method that recursively parses the query
  _parse: (query, options={}) ->
    table = options.table

    for key, value of query when key[0] isnt '$'
      throw new Error "Unexpected undefined for query key '#{key}'" if _.isUndefined(value)

      # A dot indicates a condition on a related model
      if key.indexOf('.') > 0
        @prefix_columns = true
        [cond, relation_name, relation] = @parseDotRelation(key, value)
        @join(relation_name, relation, {condition: true})
        @where.conditions.push(cond)

      # Many to Many relationships may be queried on the foreign key of the join table
      else if (reverse_relation = @model_type.reverseRelation(key)) and reverse_relation.join_table
        @prefix_columns = true
        [cond, relation_name, relation] = @parseManyToManyRelation(key, value, reverse_relation)
        @join(relation_name, relation, {pivot_only: true})
        @where.conditions.push(cond)

      else
        cond = @parseCondition(key, value, {table, method: options.method})
        @where.conditions.push(cond)

    if query?.$ids
      cond = @parseCondition('id', {$in: query.$ids}, {table})
      @where.conditions.push(cond)

    if query?.$or
      for q in query.$or
        @_parse(q, {table, method: 'orWhere'})

  parseDotRelation: (key, value) ->
    [relation_name, related_field] = key.split('.')
    relation = @getRelation(relation_name)
    cond = @parseCondition(related_field, value, {table: relation.reverse_relation.model_type.tableName()})
    return [cond, relation_name, relation]

  parseManyToManyRelation: (key, value, reverse_relation) ->
    relation = reverse_relation.reverse_relation
    relation_name = relation.key
    cond = @parseCondition(reverse_relation.foreign_key, value, {table: relation.join_table.tableName()})
    return [cond, relation_name, relation]

  parseCondition: (key, value, options={}) ->
    method = options.method || 'where'
    key = @columnName(key, options.table)

    condition = {}

    if _.isObject(value)
      condition = {method, conditions: []}

      if value?.$in
        condition.conditions.push({key, method: 'whereIn', value: value.$in})

      if value?.$nin
        condition.conditions.push({key, method: 'whereNotIn', value: value.$nin})

      if value?.$exists?
        condition.conditions.push({key, method: (if value?.$exists then 'whereNotNull' else 'whereNull')})

      # Transform a conditional of type {key: {$like: 'string'}} to ('key', 'like', '%string%')
      if _.isObject(value) and value.$like
        val = if '%' in value.$like then value.$like else "%#{value.$like}%"
        condition.conditions.push({key, method, operator: 'ilike', value: val})

      # Transform a conditional of type {key: {$lt: 5, $gt: 3}} to [('key', '<', 5), ('key', '>', 3)]
      if _.size(mongo_conditions = _.pick(value, COMPARATOR_KEYS))
        for mongo_op, val of mongo_conditions
          operator = COMPARATORS[mongo_op]

          if mongo_op is '$ne'
            if _.isNull(val)
              condition.conditions.push({key, method: "#{method}NotNull"})
            else
              condition.conditions.push({method, conditions: [
                {key, operator, method: 'orWhere', value: val}
                {key, method: 'orWhereNull'}
              ]})

          else if _.isNull(val)
            if mongo_op is '$eq'
              condition.conditions.push({key, method: "#{method}Null"})
            else
              throw new Error "Unexpected null with query key '#{key}': '#{mongo_conditions}'"

          else
            condition.conditions.push({key, operator, method, value: val})

    else
      method = "#{method}Null" if method in ['where', 'orWhere'] and _.isNull(value)
      condition = {key, value, method}

    if _.isArray(condition.conditions) and condition.conditions.length is 1
      return condition.conditions[0]

    return condition

  setSelectedColumns: () ->
    @columns = @model_type.schema().columns()
    @columns.unshift('id') unless 'id' in @columns

    if @query.$values
      @fields = if @query.$whitelist then _.intersection(@query.$values, @query.$whitelist) else @query.$values
    else if @query.$select
      @fields = if @query.$whitelist then _.intersection(@query.$select, @query.$whitelist) else @query.$select
    else if @query.$whitelist
      @fields = @query.$whitelist
    else if @query.$ids
      @fields = ['id']
    else
      @fields = @columns

    @select = if @prefix_columns then (@prefixColumn(col, @model_type.tableName()) for col in @fields) else @fields

    if @query.$include
      for key in @query.$include
        @select = @select.concat(@joins[key].columns)
