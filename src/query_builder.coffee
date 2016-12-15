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
# Make another query to get the complete set of related objects when they have been fitered by a where clause
joinToRelation = (query, relation, options={}) ->
  model_type = relation.model_type
  related_model_type = relation.reverse_model_type

  if relation.type is 'hasMany' and relation.reverse_relation.type is 'hasMany'
    pivot_table = relation.join_table.tableName()

    # Join the from model to the pivot table
    from_key = "#{model_type.tableName()}.id"
    pivot_to_key = "#{pivot_table}.#{relation.foreign_key}"
    query.leftOuterJoin(pivot_table, from_key, '=', pivot_to_key)

    unless options.pivot_only
      # Then to the to model's table (only if we need data from them second table)
      pivot_from_key = "#{pivot_table}.#{relation.reverse_relation.foreign_key}"
      to_key = "#{related_model_type.tableName()}.id"
      query.leftOuterJoin(related_model_type.tableName(), pivot_from_key, '=', to_key)

  else
    if relation.type is 'belongsTo'
      from_key = "#{model_type.tableName()}.#{relation.foreign_key}"
      to_key = "#{related_model_type.tableName()}.id"
    else
      from_key = "#{model_type.tableName()}.id"
      to_key = "#{related_model_type.tableName()}.#{relation.foreign_key}"
    query.leftOuterJoin(related_model_type.tableName(), from_key, '=', to_key)

appendWhere = (query, condition, options={}) ->
  if !_.isUndefined(condition.key)

    if condition.related

      relation_table = condition.key.split('.').shift()
      model_type = condition.related.model_type
      related_model_type = condition.related.reverse_model_type

      if condition.related.type is 'hasMany' and condition.related.reverse_relation.type is 'hasMany'
        pivot_table = condition.related.join_table.tableName()

        from_key = "#{model_type.tableName()}.id"
        pivot_to_key = "#{pivot_table}.#{condition.related.foreign_key}"

        pivot_from_key = "#{pivot_table}.#{condition.related.reverse_relation.foreign_key}"
        to_key = "#{related_model_type.tableName()}.id"
        to_table= "#{related_model_type.tableName()}"

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
        if condition.related.type is 'belongsTo'
          from_key = "#{model_type.tableName()}.#{condition.related.reverse_relation.foreign_key}"
          sub_query_select = "#{condition.related.reverse_model_type.tableName()}.id"

        else
          from_key = "#{model_type.tableName()}.id"
          sub_query_select = condition.related.reverse_relation.foreign_key

        if condition.operator
          query.whereIn(from_key, () ->
              this.select(sub_query_select).from(relation_table)[condition.method](condition.key, condition.operator, condition.value)
            )
        else
          query.whereIn(from_key, () ->
              this.select(sub_query_select).from(relation_table)[condition.method](condition.key, condition.value)
            )


    else
      if condition.operator
        query[condition.method](condition.key, condition.operator, condition.value)
      else
        query[condition.method](condition.key, condition.value)

  else if condition.conditions?.length
    query[condition.method] ->
      sub_query = @
      for c in condition.conditions
        appendWhere(sub_query, c, model_type)

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
