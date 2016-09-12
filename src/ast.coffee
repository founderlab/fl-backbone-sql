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

  constructor: ->
    @select = []
    @where = {
      conditions: [],
    }
    @join = {}
    @sort = null
    @limit = null

  print: ->
    console.log('********************** AST ******************************')

    console.log('---- Input ----')
    console.log('> query:', @query)

    console.log()

    console.log('----  AST  ----')
    console.log('> select:', @select)
    console.log('> where:')
    console.dir(@where, {depth: null, colors: true})
    console.log('> join:', @join)
    console.log('> sort:', @sort)
    console.log('> limit:', @limit)

    console.log('---------------------------------------------------------')

  # Public method that sets up for parsing
  parse: (options) ->
    @find = options.find || {}
    @cursor = options.cursor || {}
    @query = _.extend({}, @find, @cursor)
    @model_type = options.model_type

    if @cursor.$sort
      @sort = if _.isArray(@cursor.$sort) then @cursor.$sort else [@cursor.$sort]

    if @cursor.$values
      @fields = if @cursor.$whitelist then _.intersection(@cursor.$values, @cursor.$whitelist) else @cursor.$values
    else if @cursor.$select
      @fields = if @cursor.$whitelist then _.intersection(@cursor.$select, @cursor.$whitelist) else @cursor.$select
    else if @cursor.$whitelist
      @fields = @cursor.$whitelist

    @_parse(@query)

  # Internal parse method that recursively parses the query
  _parse: (query, options={}) ->

    for key, value of query when key[0] isnt '$'
      throw new Error "Unexpected undefined for query key '#{key}'" if _.isUndefined(value)

      # A dot indicates a condition on a related model
      if key.indexOf('.') > 0
        @parseDotRelation(key, value)

      # Many to Many relationships may be queried on the foreign key of the join table
      else if (reverse_relation = @model_type.reverseRelation(key)) and reverse_relation.join_table
        @parseManyToManyRelation(key, value, reverse_relation)

      else
        cond = @parseCondition(key, value, {method: options.method})
        @where.conditions.push(cond)

    # Parse conditions on related models in the same way
    # for relation, related_conditions of related_wheres
    #   conditions.related_wheres[relation] = @_parseConditions(related_conditions)

    if query?.$ids
      cond = @parseCondition(key: 'id', value: query.$ids, {method: 'whereIn'})
      @where.conditions.push(cond)

    if query?.$or
      for q in query.$or
        @_parse(q, {method: 'orWhere'})

  parseCondition: (key, value, options={}) ->
    method = options.method || 'where'
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

      # Transform a conditional of type {key: {$lt: 5}} to ('key', '<', 5)
      if _.size(mongo_conditions = _.pick(value, COMPARATOR_KEYS))
        for mongo_op, val of mongo_conditions
          throw new Error "Unexpected null with query key '#{key}': '#{mongo_conditions}'" if _.isNull(val) and (mongo_op not in ['$ne', '$eq'])
          condition.conditions.push({key, value: val, operator: COMPARATORS[mongo_op]})

    else
      method = "#{method}Null" if method in ['where', 'orWhere'] and _.isNull(value)
      condition = {key, value, method}

    return condition

  parseDotRelation: (key, value) ->
    # [relation, key] = key.split('.')
    # related_wheres[relation] or= {}
    # related_wheres[relation][key] = value

  parseManyToManyRelation: (key, value, reverse_relation) ->
    relation = reverse_relation.reverse_relation
    # conditions.joined_wheres[relation.key] or= {wheres: [], or_wheres: [], where_conditionals: []}
    # _appendCondition(conditions.joined_wheres[relation.key], key, value, method)