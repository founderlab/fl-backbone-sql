_ = require 'lodash'

module.exports = buildQueryFromAst = (query, ast, options={}) ->
  appendWhere(query, ast.where)

  for key, join of ast.joins
    join_options = {pivot_only: join.pivot_only and not (join.include or join.condition)}
    joinToRelation(query, ast.model_type, join.relation, join_options)

  return query.count('*') if ast.count or options.count
  return query.count('*').limit(1) if ast.exists or options.exists

  appendLimits(query, ast.limit, ast.offset) unless _.size(ast.joins)
  appendSelect(query, ast)
  appendSort(query, ast.sort)

  return query

# TODO: look at optimizing without left outer joins everywhere
# Make another query to get the complete set of related objects when they have been fitered by a where clause
joinToRelation = (query, model_type, relation, options={}) ->
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

appendWhere = (query, condition) ->
  if !_.isUndefined(condition.key)
    if condition.operator
      query[condition.method](condition.key, condition.operator, condition.value)
    else
      query[condition.method](condition.key, condition.value)

  else if condition.conditions?.length
    query[condition.method] ->
      sub_query = @
      for c in condition.conditions
        appendWhere(sub_query, c)

  return query

module.exports.parseSortField = parseSortField = (sort) ->
  return [sort.substr(1), 'desc'] if sort[0] is '-'
  return [sort, 'asc']

appendSelect = (query, ast) ->
  query.select(ast.select)
  return query

appendSort = (query, sort_fields) ->
  return query unless sort_fields
  for sort in sort_fields
    [col, dir] = parseSortField(sort)
    query.orderBy(col, dir)
  return query

appendLimits = (query, limit, offset) ->
  query.limit(limit) if limit
  query.offset(offset) if offset
  return query


