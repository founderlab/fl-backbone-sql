/*
 * decaffeinate suggestions:
 * DS001: Remove Babel/TypeScript constructor workaround
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
/*
  backbone-sql.js 0.6.5
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-sql
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
*/

const Knex = require('knex')
const {_, sync} = require('backbone-orm')
const Ast = require('./ast')
const buildQueryFromAst = require('./query_builder')
const Utils = require('./lib/utils')

const extractCount = function(count_json) {
  if (!(count_json != null ? count_json.length : undefined)) { return 0 }
  const count_info = count_json[0]
  return +(count_info[count_info.hasOwnProperty('count(*)') ? 'count(*)' : 'count'])
}

export default class SqlCursor extends sync.Cursor {
  constructor(...args) {
    super(...args)
    this.queryToJSON = this.queryToJSON.bind(this)
    this.runQuery = this.runQuery.bind(this)
    this.processResponse = this.processResponse.bind(this)
    this.fetchIncludedRelations = this.fetchIncludedRelations.bind(this)
    this.unjoinResults = this.unjoinResults.bind(this)
  }

  verbose = false

  execUnique = callback => {
    try {
      const ast = new Ast({
        find: this._find,
        cursor: this._cursor,
        model_type: this.model_type,
        prefix_columns: false,
      })
      let query = this.connection(this.model_type.tableName())
      query = buildQueryFromAst(query, ast, {skipSelect: true})

      if (this._cursor.$count) {
        query.count().from(this.connection.distinct(this._cursor.$unique).from(this.model_type.tableName()).as('count_query'))
        return query.asCallback((err, count_json) => callback(err, extractCount(count_json)))
      }

      // We're not selecting any fields outside of those in $unique, so we can use distinct
      if (_.difference(ast.select, this._cursor.$unique).length === 0) {
        query.distinct(ast.select)

      // Other fields are required - uses partition, a postgres window function
      } else {
        const rank_field = this._cursor.$unique[0]
        let raw_query = `${ast.select.map(s => `\"${s}\"`).join(', ')}, rank() over (partition by \"${rank_field}\"`
        if (ast.sort != null ? ast.sort.length : undefined) {
          let sort
          if (sort = ast.sort != null ? ast.sort.shift() : undefined) {
            raw_query += ` order by \"${sort.column}\" ${sort.direction}`
          }
          for (sort of Array.from((ast.sort != null))) {
            raw_query += ` , \"${sort.column}\" ${sort.direction}`
          }
        }
        raw_query += ')'
        const subquery = this.connection.select(this.connection.raw(raw_query))
        subquery.from(this.model_type.tableName()).as('subquery')
        query.select(ast.select).from(subquery).where('rank', 1)
      }

      return this.runQuery(query, ast, callback)

    } catch (error) {
      const err = error
      return callback(new Error(`Query failed for model: ${this.model_type.model_name} with error: ${err}`))
    }
  }

  queryToJSON = callback => {
    let ast, query
    if (this.hasCursorQuery('$zero')) { return callback(null, this.hasCursorQuery('$one') ? null : []) }

    if (!this.verbose) { this.verbose = this._cursor.$verbose }
    if (this.verbose) {
      this.start_time = new Date().getTime()
    }

    if (this.hasCursorQuery('$count')) { this._cursor.$count = true }
    if (this.hasCursorQuery('$exists')) { this._cursor.$exists = true }

    // Unique
    if (this._cursor.$unique) { return this.execUnique(callback) }

    try {
      ast = new Ast({
        find: this._find,
        cursor: this._cursor,
        model_type: this.model_type,
      })
      query = this.connection(this.model_type.tableName())
      query = buildQueryFromAst(query, ast)

      // $in : [] or another query that would result in an empty result set in mongo has been given
      if (ast.abort) { return callback(null, this._cursor.$count ? 0 : (this._cursor.$one ? null : [])) }

    } catch (err) {
      return callback(`Query failed for model: ${this.model_type.model_name} with error: ${err}`)
    }

    return this.runQuery(query, ast, callback)
  }

  runQuery = (query, ast, _callback) => {
    let callback = _callback
    if (this.verbose) {
      this.query_ready_time = new Date().getTime()

      console.log('\n----------')
      ast.print()
      console.dir(query.toString(), {depth: null, colors: true})
      console.log('Built in', this.query_ready_time-this.start_time, 'ms')
      console.log('----------')
      callback = (err, res) => {
        console.log('Query complete in', new Date().getTime() - this.start_time, 'ms')
        return _callback(err, res)
      }
    }

    return query.asCallback((err, json) => {
      if (err) { return callback(new Error(`Query failed for model: ${this.model_type.model_name} with error: ${err}`)) }

      if (this.hasCursorQuery('$count') || this.hasCursorQuery('$exists')) {
        const count = extractCount(json)
        return callback(null, this.hasCursorQuery('$count') ? count : (count > 0))
      }

      if (ast.prefix_columns) { json = this.unjoinResults(json, ast) }

      if (ast.joinedIncludesWithConditions().length) {
        return this.fetchIncludedRelations(json, ast, callback)
      } else {
        return this.processResponse(json, ast, callback)
      }
    })
  }

  // Process any remaining queries and return the json
  processResponse = (json, ast, callback) => {
    const schema = this.model_type.schema()

    for (const model_json of Array.from(json)) { this.backbone_adapter.nativeToAttributes(model_json, schema) }
    json = this.selectResults(json)

    // NOTE: limit and offset would apply to the join table so do as post-process. TODO: optimize
    if (this._cursor.$include) {
      if (this._cursor.$offset) {
        let number = json.length - this._cursor.$offset
        if (number < 0) { number = 0 }
        json = number ? json.slice(this._cursor.$offset, this._cursor.$offset+number) : []
      }

      if (this._cursor.$limit) {
        json = json.splice(0, Math.min(json.length, this._cursor.$limit))
      }
    }

    if (this.hasCursorQuery('$page')) {
      let query = this.connection(this.model_type.tableName())
      query = buildQueryFromAst(query, ast, {count: true})

      if (this._cursor.$unique) {
        const subquery = this.connection.distinct(this._cursor.$unique)
        subquery.from(this.model_type.tableName()).as('subquery')
        query.from(subquery)
      } else {
        query.from(this.model_type.tableName())
      }

      if (this.verbose) {
        console.log('\n---------- counting rows for $page ----------')
        console.dir(query.toString(), {colors: true})
        console.log('---------------------------------------------')
      }

      return query.asCallback((err, count_json) => {
        if (err) { return callback(err) }
        return callback(null, {
          offset: this._cursor.$offset || 0,
          total_rows: extractCount(count_json),
          rows: json,
        })
      })
    } else {
      return callback(null, json)
    }
  }

  // Make another query to get the complete set of related objects when they have been fitered by a where clause
  fetchIncludedRelations = (json, ast, callback) => {
    const relation_ast = new Ast({
      model_type: this.model_type,
      query: {
        id: {$in: _.pluck(json, 'id')},
        $select: ['id'],
        $include: (Array.from(ast.joinedIncludesWithConditions()).map((j) => j.key)),
      },
    })
    let relation_query = this.connection(this.model_type.tableName())
    relation_query = buildQueryFromAst(relation_query, relation_ast)

    return relation_query.asCallback((err, raw_relation_json) => {
      if (err) { return callback(err) }
      const relation_json = this.unjoinResults(raw_relation_json, relation_ast)
      for (const placeholder of Array.from(relation_json)) {
        const model = _.find(json, test => test.id === placeholder.id)
        _.extend(model, placeholder)
      }
      return this.processResponse(json, ast, callback)
    })
  }

  // Rows returned from a join query need to be un-merged into the correct json format
  unjoinResults = (raw_json, ast) => {
    if (!raw_json || !raw_json.length) { return raw_json }
    const json = []
    const { model_type } = ast

    for (const row of Array.from(raw_json)) {
      let found, related_json, relation_key
      let model_json = {}
      const row_relation_json = {}

      // Fields are prefixed with the table name of the model they belong to so we can test which the values are for
      // and assign them to the correct object
      for (const key in row) {
        let match
        const value = row[key]
        if (match = ast.prefixRegex().exec(key)) {
          model_json[match[1]] = value

        } else {
          for (relation_key in ast.joins) {
            const join = ast.joins[relation_key]
            if (join.include) {
              related_json = (row_relation_json[relation_key] || (row_relation_json[relation_key] = {}))
              if (match = ast.prefixRegex(join.relation.reverse_model_type.tableName()).exec(key)) {
                related_json[match[1]] = value
                found = true
              }
            }
          }
          if (!found) {
            model_json[key] = value
          }
        }
      }

      // If there was a hasMany relationship or multiple $includes we'll have multiple rows for each model
      if ((found = _.find(json, test => test.id === model_json.id))) {
        model_json = found
      // Add this model to the result if we haven't already
      } else {
        json.push(model_json)
      }

      // Add relations to the model_json if included
      for (relation_key in row_relation_json) {

        related_json = row_relation_json[relation_key]
        if (_.isNull(related_json.id)) {
          if (model_type.relation(relation_key).type === 'hasMany') {
            model_json[relation_key] = []
          } else {
            model_json[relation_key] = null
          }

        } else if (!_.isEmpty(related_json)) {
          const reverse_relation_schema = model_type.relation(relation_key).reverse_model_type.schema()
          related_json = this.backbone_adapter.nativeToAttributes(related_json, reverse_relation_schema)

          if (model_type.relation(relation_key).type === 'hasMany') {
            if (!model_json[relation_key]) { model_json[relation_key] = [] }
            if (!_.find(model_json[relation_key], test => test.id === related_json.id)) { model_json[relation_key].push(related_json) }
          } else {
            model_json[relation_key] = related_json
          }
        }
      }
    }

    return json
  }

  selectResults = json => {
    let item
    if (this._cursor.$one) { json = json.slice(0, 1) }

    // TODO: OPTIMIZE TO REMOVE 'id' and '_rev' if needed
    if (this._cursor.$values) {
      let key
      const $values = this._cursor.$whitelist ? _.intersection(this._cursor.$values, this._cursor.$whitelist) : this._cursor.$values
      if (this._cursor.$values.length === 1) {
        key = this._cursor.$values[0]
        json = $values.length ? ((() => {
          const result = []
          for (item of Array.from(json)) {               if (item.hasOwnProperty(key)) { result.push(item[key]) } else { result.push(null) }
          }
          return result
        })()) : _.map(json, () => null)
      } else {
        json = ((() => {
          const result1 = []
          for (item of Array.from(json)) {               result1.push((((() => {
            const result2 = []
            for (key of Array.from($values)) {                   if (item.hasOwnProperty(key)) {
              result2.push(item[key])
            }
            }
            return result2
          })())))
          }
          return result1
        })())
      }

    } else if (this._cursor.$select) {
      let $select = (Array.from(this._cursor.$select).map((field) => (Array.from(field).includes('.') ? field.split('.').pop() : field)))
      if (this._cursor.$whitelist) { $select = _.intersection($select, this._cursor.$whitelist) }
      json = ((() => {
        const result3 = []
        for (item of Array.from(json)) {             result3.push(_.pick(item, $select))
        }
        return result3
      })())

    } else if (this._cursor.$whitelist) {
      json = ((() => {
        const result4 = []
        for (item of Array.from(json)) {             result4.push(_.pick(item, this._cursor.$whitelist))
        }
        return result4
      })())
    }

    if (this.hasCursorQuery('$page')) { return json } // paging expects an array
    if (this._cursor.$one) { return (json[0] || null) } else { return json }
  }
}
