/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
let buildQueryFromAst
const _ = require('lodash')

module.exports = (buildQueryFromAst = function(query, ast, options) {
  if (options == null) { options = {} }
  appendWhere(query, ast.where)

  let hasInclude = false

  for (const key in ast.joins) {
    const join = ast.joins[key]
    const join_options = {pivot_only: join.pivot_only && !(join.include || join.condition)}
    if (join.include) {
      joinToRelation(query, join.relation, join_options)
      hasInclude = true
    }
  }

  if (ast.count || options.count) { return query.count('*') }
  if (ast.exists || options.exists) { return query.count('*').limit(1) }

  if (!hasInclude) { appendLimits(query, ast.limit, ast.offset) } //does not apply limit and offset clauses for queries with $include
  if (!options.skipSelect) { appendSelect(query, ast) }
  appendSort(query, ast)

  return query
})

// TODO: look at optimizing without left outer joins everywhere
// Make another query to get the complete set of relation objects when they have been fitered by a where clause
const joinToRelation = function(query, relation, options) {
  let from_key, to_key
  if (options == null) { options = {} }
  const { model_type } = relation
  const relation_model_type = relation.reverse_model_type

  const from_table = model_type.tableName()
  const to_table = relation_model_type.tableName()

  if ((relation.type === 'hasMany') && (relation.reverse_relation.type === 'hasMany')) {
    const pivot_table = relation.join_table.tableName()

    // Join the from model to the pivot table
    from_key = `${from_table}.id`
    const pivot_to_key = `${pivot_table}.${relation.foreign_key}`
    query.leftOuterJoin(pivot_table, from_key, '=', pivot_to_key)

    if (!options.pivot_only) {
      // Then to the to model's table (only if we need data from them second table)
      const pivot_from_key = `${pivot_table}.${relation.reverse_relation.foreign_key}`
      to_key = `${to_table}.id`
      return query.leftOuterJoin(to_table, pivot_from_key, '=', to_key)
    }

  } else {
    if (relation.type === 'belongsTo') {
      from_key = `${from_table}.${relation.foreign_key}`
      to_key = `${to_table}.id`
    } else {
      from_key = `${from_table}.id`
      to_key = `${to_table}.${relation.foreign_key}`
    }
    return query.leftOuterJoin(to_table, from_key, '=', to_key)
  }
}

const appendRelatedWhere = function(query, condition, options) {
  let from_key, select
  if (options == null) { options = {} }
  const from_model_type = condition.relation.model_type
  const table = condition.model_type.tableName()

  if (condition.relation.type === 'belongsTo') {
    from_key = `${from_model_type.tableName()}.${condition.relation.reverse_relation.foreign_key}`
    select = `${condition.relation.reverse_model_type.tableName()}.id`

  } else {
    from_key = `${from_model_type.tableName()}.id`
    select = condition.relation.reverse_relation.foreign_key
  }

  const in_method = condition.method === 'orWhere' ? 'orWhereIn' : 'whereIn'
  if (condition.operator) {
    return query[in_method](from_key, function() {
      const q = this
      if (condition.value) {
        return this.select(select).from(table)[condition.method](condition.key, condition.operator, condition.value)
      } else if (condition.dot_where) {
        this.select(select).from(table)
        return appendRelatedWhere(q, condition.dot_where, options)
      }
    })

  } else {
    return query[in_method](from_key, function() {
      const q = this
      if (condition.value) {
        return this.select(select).from(table)[condition.method](condition.key, condition.value)
      } else if (condition.dot_where) {
        this.select(select).from(table)
        return appendRelatedWhere(q, condition.dot_where, options)
      }
    })
  }
}

const appendWhere = function(query, condition, options) {
  if (options == null) { options = {} }
  if (!_.isUndefined(condition.key) || condition.dot_where) {

    if (condition.relation) {
      if ((condition.relation.type === 'hasMany') && (condition.relation.reverse_relation.type === 'hasMany')) {

        const relation_table = condition.key.split('.').shift()
        const from_model_type = condition.relation.model_type
        const relation_model_type = condition.relation.reverse_model_type

        const from_table = from_model_type.tableName()
        let to_table = relation_model_type.tableName()
        const pivot_table = condition.relation.join_table.tableName()

        const from_key = `${from_table}.id`
        const pivot_to_key = `${pivot_table}.${condition.relation.foreign_key}`

        const pivot_from_key = `${pivot_table}.${condition.relation.reverse_relation.foreign_key}`
        const to_key = `${to_table}.id`
        to_table= `${to_table}`

        if (condition.operator) {
          query.whereIn(from_key, function() {
            return this.select(pivot_to_key).from(pivot_table).whereIn(pivot_from_key, function() {
              return this.select('id').from(to_table)[condition.method](condition.key, condition.operator, condition.value)
            })
          })
        } else {
          query.whereIn(from_key, function() {
            return this.select(pivot_to_key).from(pivot_table).whereIn(pivot_from_key, function() {
              return this.select('id').from(to_table)[condition.method](condition.key, condition.value)
            })
          })
        }

      } else {
        appendRelatedWhere(query, condition, options)
      }

    } else {
      if (condition.operator) {
        query[condition.method](condition.key, condition.operator, condition.value)
      } else {
        query[condition.method](condition.key, condition.value)
      }
    }

  } else if (condition.conditions != null ? condition.conditions.length : undefined) {
    query[condition.method](function() {
      const sub_query = this
      return Array.from(condition.conditions).map((c) =>
        appendWhere(sub_query, c))
    })
  }

  return query
}

const appendSelect = function(query, ast) {
  query.select(ast.select)
  return query
}

const appendSort = function(query, ast) {
  if (!ast.sort) { return query }
  for (const sort of Array.from(ast.sort)) { query.orderBy(sort.column, sort.direction) }
  return query
}

const appendLimits = function(query, limit, offset) {
  if (limit) { query.limit(limit) }
  if (offset) { query.offset(offset) }
  return query
}
