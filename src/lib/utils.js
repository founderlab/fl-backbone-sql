/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
let Utils;
const {DatabaseURL} = require('backbone-orm');

const PROTOCOLS = {
  'mysql:': 'mysql', 'mysql2:': 'mysql',
  'postgres:': 'postgres', 'pg:': 'postgres',
  'sqlite:': 'sqlite3', 'sqlite3:': 'sqlite3'
};

module.exports = (Utils = class Utils {
  static protocolType(url) {
    if (!url.protocol) { url = new DatabaseURL(url); }
    return PROTOCOLS[url.protocol];
  }

  static parseSortField(sort) {
    if (sort[0] === '-') { return [sort.substr(1), 'desc']; }
    return [sort, 'asc'];
  }
});
