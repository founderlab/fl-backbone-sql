/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
/*
  backbone-sql.js 0.6.5
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-sql
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
*/

let DatabaseTools
const Knex = require('knex')
const {_, Queue} = require('backbone-orm')

const KNEX_COLUMN_OPTIONS = ['textType', 'length', 'precision', 'scale', 'value', 'values']

// TODO: when knex fixes join operator, remove this deprecation warning
const knex_helpers = require('knex/lib/helpers')
const KNEX_SKIP = ['The five argument join']
const _deprecate = knex_helpers.deprecate
knex_helpers.deprecate = function(msg) { if (msg.indexOf(KNEX_SKIP) !== 0) { return _deprecate.apply(this, _.toArray(arguments)) } }

const KNEX_TYPES = {
  datetime: 'dateTime',
  biginteger: 'bigInteger',
}

module.exports = (DatabaseTools = class DatabaseTools {

  constructor(connection, table_name, schema, options) {
    this.resetSchema = this.resetSchema.bind(this)
    this.ensureSchema = this.ensureSchema.bind(this)
    this.createOrUpdateTable = this.createOrUpdateTable.bind(this)
    this.addColumn = this.addColumn.bind(this)
    this.updateColumn = this.updateColumn.bind(this)
    this.hasColumn = this.hasColumn.bind(this)
    this.hasTable = this.hasTable.bind(this)
    this.dropTable = this.dropTable.bind(this)
    this.dropTableIfExists = this.dropTableIfExists.bind(this)
    this.renameTable = this.renameTable.bind(this)
    this.connection = connection
    this.table_name = table_name
    this.schema = schema
    if (options == null) { options = {} }
  }

  resetSchema(options, callback) {
    if (arguments.length === 1) { [callback, options] = Array.from([options, {}]) }
    if (this.resetting) { return callback() }
    this.resetting = true
    const queue = new Queue(1)
    queue.defer(callback => this.connection.knex().schema.dropTableIfExists(this.table_name).asCallback(callback))
    queue.defer(callback => {
      const join_queue = new Queue(1)
      for (const join_table of Array.from(this.schema.joinTables())) {
        (join_table => join_queue.defer(callback => join_table.db().resetSchema(callback)))(join_table)
      }
      return join_queue.await(callback)
    })
    return queue.await(err => {
      this.resetting = false; if (err) { return callback(err) }
      return this.ensureSchema(options, callback)
    })
  }

  // Ensure that the schema is reflected correctly in the database
  // Will create a table and add columns as required will not remove columns (TODO)
  ensureSchema(options, callback) {
    if (arguments.length === 1) { [callback, options] = Array.from([options, {}]) }

    if (this.ensuring) { return callback() }
    this.ensuring = true

    const queue = new Queue(1)
    queue.defer(callback => this.createOrUpdateTable(options, callback))
    queue.defer(callback => {
      const join_queue = new Queue(1)
      for (const join_table of Array.from(this.schema.joinTables())) {
        (join_table => join_queue.defer(callback => join_table.db().ensureSchema(callback)))(join_table)
      }
      return join_queue.await(callback)
    })

    return queue.await(err => { this.ensuring = false; return callback(err) })
  }

  createOrUpdateTable(options, callback) {
    this.hasTable((err, table_exists) => {
      let column_info, type
      if (err) { return callback(err) }
      if (options.verbose) { console.log(`Ensuring table: ${this.table_name} (exists: ${!!table_exists}) with fields: '${_.keys(this.schema.fields).join(', ')}' and relations: '${_.keys(this.schema.relations).join(', ')}'`) }

      const columns = []

      // look up the add or update columns
      // NOTE: Knex requires the add an update operations to be performed within the table function.
      // This means that hasColumn being asynchronous requires the check to be done before calling the table function
      for (var key of Array.from(this.schema.columns())) {
        let field
        if (field = this.schema.fields[key]) {
          let override
          if (override = KNEX_TYPES[(type = field.type.toLowerCase())]) { type = override }
          columns.push({key, type, options: field})
        } else if (key === 'id') {
          columns.push({key, type: 'increments', options: {indexed: true, primary: true}})
        }
      }

      for (key in this.schema.relations) {
        const relation = this.schema.relations[key]
        if ((relation.type === 'belongsTo') && !relation.isVirtual() && !relation.isEmbedded()) {
          ((key, relation) => columns.push({key: relation.foreign_key, type: 'integer', options: {indexed: true, nullable: true}}))(key, relation)
        }
      }

      const group = (columns, callback) => {
        if (!table_exists) { return callback(null, {add: columns, update: []}) }

        const result = {add: [], update: []}

        const queue = new Queue()
        for (column_info of Array.from(columns)) {
          (column_info => queue.defer(callback => {
            return this.hasColumn(column_info.key, (err, exists) => {
              if (err) { return callback(err) }
              (exists ? result.update : result.add).push(column_info); return callback()
            })
          }))(column_info)
        }
        return queue.await(err => callback(err, result))
      }

      return group(columns, (err, result) => {
        if (err) { return callback(err) }
        return this.connection.knex().schema[table_exists ? 'table' : 'createTable'](this.table_name, table => {
          for (column_info of Array.from(result.add)) { this.addColumn(table, column_info) }
          return (() => {
            const result1 = []
            for (column_info of Array.from(result.update)) {               result1.push(this.updateColumn(table, column_info))
            }
            return result1
          })()
        }).asCallback(callback)
      })
    })
  }

  addColumn(table, column_info) {
    const column_args = [column_info.key]

    // Assign column specific arguments
    const constructor_options = _.pick(column_info.options, KNEX_COLUMN_OPTIONS)
    let column_method = column_info.type

    if (!_.isEmpty(constructor_options)) {
      // Special case as they take two args
      if (['float', 'decimal'].includes(column_method)) {
        column_args[1] = constructor_options['precision']
        column_args[2] = constructor_options['scale']
      // Assume we've been given one valid argument
      } else {
        column_args[1] = _.values(constructor_options)[0]
      }
    }

    // Use jsonb
    if (['json'].includes(column_method)) {
      column_method = 'jsonb'
    }

    const column = table[column_method].apply(table, column_args)
    if (!!column_info.options.nullable) { column.nullable() }
    if (!!column_info.options.primary) { column.primary() }
    if (!!column_info.options.indexed) { column.index() }
    if (!!column_info.options.unique) { column.unique() }

  }

  // TODO: handle column type changes and figure out how to update columns properly
  updateColumn(table, column_info) {
    // table.index(column_info.key) if column_info.options.indexed # fails if the column already exists
    // table.unique(column_info.key) if column_info.options.unique
  }

  // knex method wrappers
  hasColumn(column, callback) { return this.connection.knex().schema.hasColumn(this.table_name, column).asCallback(callback) }
  hasTable(callback) { return this.connection.knex().schema.hasTable(this.table_name).asCallback(callback) }
  dropTable(callback) { return this.connection.knex().schema.dropTable(this.table_name).asCallback(callback) }
  dropTableIfExists(callback) { return this.connection.knex().schema.dropTableIfExists(this.table_name).asCallback(callback) }
  renameTable(to, callback) { return this.connection.knex().schema.renameTable(this.table_name, to).asCallback(callback) }
})
