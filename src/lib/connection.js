/*
 * decaffeinate suggestions:
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
/*
  backbone-sql.js 0.6.5
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-sql
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
*/

let Connection
const Knex = require('knex')
const {_, Queue, DatabaseURL, ConnectionPool} = require('backbone-orm')

const Utils = require('./utils')

class KnexConnection {
  constructor(knex) {
    this.knex = knex
  }
  destroy() {} // TODO: look for a way to close knex
}

module.exports = (Connection = class Connection {
  constructor(full_url) {
    let connection_info, protocol
    const database_url = new DatabaseURL(full_url)
    this.url = database_url.format({exclude_table: true, exclude_query: true}) // pool the raw endpoint without the table
    if (this.knex_connection = ConnectionPool.get(this.url)) { return } // found in pool

    if (!(protocol = Utils.protocolType(database_url))) { throw `Unrecognized sql variant: ${full_url} for protocol: ${database_url.protocol}` }

    if (protocol === 'sqlite3') {
      connection_info = {filename: database_url.host || ':memory:'}
    } else {
      connection_info = _.extend({host: database_url.hostname, database: database_url.database, charset: 'utf8'}, database_url.parseAuth() || {})
    }
    const knex = Knex({client: protocol, connection: connection_info})
    ConnectionPool.set(this.url, (this.knex_connection = new KnexConnection(knex)))
  }

  knex() { return (this.knex_connection != null ? this.knex_connection.knex : undefined) }
})
