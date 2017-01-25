_ = require 'lodash'

module.exports = buildQueryFromAst = (query, ast, options={}) ->
  appendWhere(query, ast.where)

  hasInclude = false

  for key, join of ast.joins
    join_options = {pivot_only: join.pivot_only and not (join.include or join.condition)}
    if join.include
      joinToRelation(query, join.relation, join_options)
      hasInclude = true

  return query.count('*') if ast.count or options.count
  return query.count('*').limit(1) if ast.exists or options.exists

  appendLimits(query, ast.limit, ast.offset) unless hasInclude #does not apply limit and offset clauses for queries with $include
  appendSelect(query, ast)
  appendSort(query, ast)

  return query

# TODO: look at optimizing without left outer joins everywhere
# Make another query to get the complete set of relation objects when they have been fitered by a where clause
joinToRelation = (query, relation, options={}) ->
  model_type = relation.model_type
  relation_model_type = relation.reverse_model_type

  from_table = model_type.tableName()
  to_table = relation_model_type.tableName()

  if relation.type is 'hasMany' and relation.reverse_relation.type is 'hasMany'
    pivot_table = relation.join_table.tableName()

    # Join the from model to the pivot table
    from_key = "#{from_table}.id"
    pivot_to_key = "#{pivot_table}.#{relation.foreign_key}"
    query.leftOuterJoin(pivot_table, from_key, '=', pivot_to_key)

    unless options.pivot_only
      # Then to the to model's table (only if we need data from them second table)
      pivot_from_key = "#{pivot_table}.#{relation.reverse_relation.foreign_key}"
      to_key = "#{to_table}.id"
      query.leftOuterJoin(to_table, pivot_from_key, '=', to_key)

  else
    if relation.type is 'belongsTo'
      from_key = "#{from_table}.#{relation.foreign_key}"
      to_key = "#{to_table}.id"
    else
      from_key = "#{from_table}.id"
      to_key = "#{to_table}.#{relation.foreign_key}"
    query.leftOuterJoin(to_table, from_key, '=', to_key)

appendRelatedWhere = (query, condition, options={}) ->
  from_model_type = condition.relation.model_type
  table = condition.model_type.tableName()

  if condition.relation.type is 'belongsTo'
    from_key = "#{from_model_type.tableName()}.#{condition.relation.reverse_relation.foreign_key}"
    select = "#{condition.relation.reverse_model_type.tableName()}.id"

  else
    from_key = "#{from_model_type.tableName()}.id"
    select = condition.relation.reverse_relation.foreign_key

  if condition.operator
    query.whereIn(from_key, () ->
      q = @
      if condition.value
        this.select(select).from(table)[condition.method](condition.key, condition.operator, condition.value)
      else if condition.dot_where
        this.select(select).from(table)
        appendRelatedWhere(q, condition.dot_where, options)
    )

  else
    query.whereIn(from_key, () ->
      q = @
      if condition.value
        this.select(select).from(table)[condition.method](condition.key, condition.value)
      else if condition.dot_where
        this.select(select).from(table)
        appendRelatedWhere(q, condition.dot_where, options)
    )

appendWhere = (query, condition, options={}) ->
  if !_.isUndefined(condition.key) or condition.dot_where

    if condition.relation
      if condition.relation.type is 'hasMany' and condition.relation.reverse_relation.type is 'hasMany'

        relation_table = condition.key.split('.').shift()
        from_model_type = condition.relation.model_type
        relation_model_type = condition.relation.reverse_model_type

        from_table = from_model_type.tableName()
        to_table = relation_model_type.tableName()
        pivot_table = condition.relation.join_table.tableName()

        from_key = "#{from_table}.id"
        pivot_to_key = "#{pivot_table}.#{condition.relation.foreign_key}"

        pivot_from_key = "#{pivot_table}.#{condition.relation.reverse_relation.foreign_key}"
        to_key = "#{to_table}.id"
        to_table= "#{to_table}"

        if condition.operator
          query.whereIn(from_key, () ->
            this.select(pivot_to_key).from(pivot_table).whereIn(pivot_from_key, () ->
              this.select('id').from(to_table)[condition.method](condition.key, condition.operator, condition.value)
            )
          )
        else
          query.whereIn(from_key, () ->
            this.select(pivot_to_key).from(pivot_table).whereIn(pivot_from_key, () ->
              this.select('id').from(to_table)[condition.method](condition.key, condition.value)
            )
          )

      else
        appendRelatedWhere(query, condition, options)

    else
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

appendSelect = (query, ast) ->
  query.select(ast.select)
  return query

appendSort = (query, ast) ->
  return query unless ast.sort
  query.orderBy(sort.column, sort.direction) for sort in ast.sort
  return query

appendLimits = (query, limit, offset) ->
  query.limit(limit) if limit
  query.offset(offset) if offset
  return query
