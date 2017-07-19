/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS104: Avoid inline assignments
 * DS204: Change includes calls to have a more natural evaluation order
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
/*
  backbone-sql.js 0.6.5
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-sql
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
*/

let BackboneORM
const {Backbone, Queue, Schema, Utils, JSONUtils, DatabaseURL} = (BackboneORM = require('backbone-orm'))
const _ = require('lodash')

const SqlCursor = require('./cursor')
const DatabaseTools = require('./database_tools')
const Connection = require('./lib/connection')
const SQLUtils = require('./lib/utils')

const DESTROY_BATCH_LIMIT = 1000
const CAPABILITIES = {
  mysql: {embed: false, json: false, unique: false, manual_ids: false, dynamic: false, self_reference: false}, // TODO: fix self-reference test and remove as a capabilty from all syncs
  postgres: {embed: false, json: true, unique: true, manual_ids: false, dynamic: false, self_reference: false}, // TODO: fix self-reference test and remove as a capabilty from all syncs
  sqlite: {embed: false, json: false, unique: false, manual_ids: false, dynamic: false, self_reference: false}, // TODO: fix self-reference test and remove as a capabilty from all syncs
}

class SqlSync {

  constructor(model_type, options) {
    this.create = this.create.bind(this)
    this.update = this.update.bind(this)
    this.deleteCB = this.deleteCB.bind(this)
    this.getTable = this.getTable.bind(this)
    this.getConnection = this.getConnection.bind(this)
    this.db = this.db.bind(this)
    this.model_type = model_type
    if (options == null) { options = {} }
    for (const key in options) { const value = options[key]; this[key] = value }
    this.model_type.model_name = Utils.findOrGenerateModelName(this.model_type)
    this.schema = new Schema(this.model_type, {id: {type: 'Integer'}})

    this.backbone_adapter = require('./lib/backbone_adapter')
  }

  // @no_doc
  initialize() {
    let url
    if (this.is_initialized) { return } this.is_initialized = true

    this.schema.initialize()
    if (!(url = _.result(new this.model_type(), 'url'))) { throw new Error('Missing url for model') }
    return this.connect(url)
  }

  //##################################
  // Classic Backbone Sync
  //##################################

  // @no_doc
  read(model, options) {
    // a collection
    if (model.models) {
      return this.cursor().toJSON((err, json) => {
        if (err) { return options.error(err) }
        if (!json) { return options.error(new Error('Collection not fetched')) }
        return (typeof options.success === 'function' ? options.success(json) : undefined)
      })
      // a model
    } else {
      return this.cursor(model.id).toJSON((err, json) => {
        if (err) { return options.error(err) }
        if (!json) { return options.error(new Error(`Model not found. Id ${model.id}`)) }
        return options.success(json)
      })
    }
  }

  // @no_doc
  create(model, options) {
    const json = model.toJSON()
    const save_json = this.parseJSON(json)
    return this.getTable('master').insert(save_json, 'id').asCallback((err, res) => {
      if (err) { return options.error(err) }
      if (!(res != null ? res.length : undefined)) { return options.error(new Error(`Failed to create model with attributes: ${JSONUtils.stringify(model.attributes)}`)) }
      json.id = res[0]
      return options.success(this.backbone_adapter.nativeToAttributes(json, this.schema))
    })
  }

  // @no_doc
  update(model, options) {
    const json = model.toJSON()
    const save_json = this.parseJSON(json)
    return this.getTable('master').where('id', model.id).update(save_json).asCallback((err, res) => {
      if (err) { return options.error(err) }
      return options.success(this.backbone_adapter.nativeToAttributes(json, this.schema))
    })
  }

  // @nodoc
  delete(model, options) { return this.deleteCB(model, err => err ? options.error(err) : options.success()) }

  // @nodoc
  deleteCB(model, callback) {
    return this.getTable('master').where('id', model.id).del().asCallback((err, res) => {
      if (err) { return callback(err) }
      return Utils.patchRemove(this.model_type, model, callback)
    })
  }

  //##################################
  // Backbone ORM - Class Extensions
  //##################################

  parseJSON(_json) {
    const json = _.clone(_json)
    for (const key in this.schema.fields) {
      let needle
      const value = this.schema.fields[key]
      if ((needle = value.type != null ? value.type.toLowerCase() : undefined, ['json', 'jsonb'].includes(needle)) && json[key]) {
        json[key] = JSON.stringify(json[key])
      }
    }
    return json
  }

  // @no_doc
  resetSchema(options, callback) { return this.db().resetSchema(options, callback) }

  // @no_doc
  cursor(query) {
    if (query == null) { query = {} }
    const options = _.pick(this, ['model_type', 'backbone_adapter'])
    options.connection = this.getConnection()
    return new SqlCursor(query, options)
  }

  // @no_doc
  destroy(query, callback) {
    if (arguments.length === 1) { [query, callback] = Array.from([{}, query]) }
    return this.model_type.each(_.extend({$each: {limit: DESTROY_BATCH_LIMIT, json: true}}, query), this.deleteCB, callback)
  }

  //##################################
  // Backbone SQL Sync - Custom Extensions
  //##################################

  // @no_doc
  connect(url) {
    this.table = (new DatabaseURL(url)).table
    if (!this.connections) { this.connections = {all: [], master: new Connection(url), slaves: []} }

    if (this.slaves != null ? this.slaves.length : undefined) {
      for (const slave_url of Array.from(this.slaves)) { this.connections.slaves.push(new Connection(`${slave_url}/${this.table}`)) }
    }

    // cache all connections
    this.connections.all = [this.connections.master].concat(this.connections.slaves)
    return this.schema.initialize()
  }

  // Get the knex table instance for a db_type
  getTable(db_type) { return this.getConnection(db_type)(this.table) }

  // Return the master db connection if db_type is 'master' or a random one otherwise
  getConnection(db_type) {
    if ((db_type === 'master') || (this.connections.all.length === 1)) { return this.connections.master.knex() }
    return this.connections.all[~~(Math.random() * (this.connections.all.length))].knex()
  }

  db() { return this.db_tools || (this.db_tools = new DatabaseTools(this.connections.master, this.table, this.schema)) }
}

module.exports = function(type, options) {
  let sync_fn
  if (Utils.isCollection(new type())) { // collection
    const model_type = Utils.configureCollectionModelType(type, module.exports)
    return type.prototype.sync = model_type.prototype.sync
  }

  const sync = new SqlSync(type, options)
  type.prototype.sync = (sync_fn = function(method, model, options) { // save for access by model extensions
    if (options == null) { options = {} }
    sync.initialize()
    if (method === 'createSync') { return module.exports.apply(null, Array.prototype.slice.call(arguments, 1)) } // create a new sync
    if (method === 'sync') { return sync }
    if (method === 'db') { return sync.db() }
    if (method === 'schema') { return sync.schema }
    if (method === 'isRemote') { return false }
    if (method === 'tableName') { return sync.table }
    if (sync[method]) { return sync[method].apply(sync, Array.prototype.slice.call(arguments, 1)) } else { return undefined }
  })

  Utils.configureModelType(type) // mixin extensions
  return BackboneORM.model_cache.configureSync(type, sync_fn)
}

module.exports.capabilities = url => CAPABILITIES[SQLUtils.protocolType(url)] || {}
