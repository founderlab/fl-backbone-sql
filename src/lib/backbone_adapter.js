/*
 * decaffeinate suggestions:
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

let SqlBackboneAdapter;
const {_} = require('backbone-orm');

module.exports = (SqlBackboneAdapter = class SqlBackboneAdapter {
  static nativeToAttributes(json, schema) {
    let value;
    for (var key in schema.fields) {
      var needle;
      value = schema.fields[key];
      if (schema.fields[key] && (schema.fields[key].type === 'Boolean') && (json[key] !== null)) {
        json[key] = !!json[key];
      } else if (((value.type != null ? value.type.toLowerCase() : undefined) === 'json') && json[key] && _.isString(json[key])) {
        try {
          json[key] = JSON.parse(json[key]);
        } catch (err) {}
          // console.log(err)
      } else if ((needle = value.type != null ? value.type.toLowerCase() : undefined, ['float', 'decimal'].includes(needle)) && json[key] && _.isString(json[key])) {
        json[key] = +json[key];
      }
    }

    // Make join table ids strings
    for (key in json) {
      value = json[key];
      if (key.endsWith('_id') && value) {
        json[key] = value.toString();
      }
    }

    // Make primary key and foreign keys strings
    if (json.id) { json.id = json.id.toString(); }
    for (key in schema.relations) {
      var foreign_key;
      const relation = schema.relations[key];
      if (relation.type === 'belongsTo') {
        ({ foreign_key } = relation);
      }
      if (foreign_key && json[foreign_key]) {
        json[foreign_key] = json[foreign_key].toString();
      }
    }

    return json;
  }
});
