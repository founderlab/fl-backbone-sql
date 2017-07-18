/*
  backbone-sql.js 0.6.5
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-sql
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
*/

let BackboneORM, BackboneSQL;
const {_, Backbone} = (BackboneORM = require('backbone-orm'));

module.exports = (BackboneSQL = require('./core')); // avoid circular dependencies
const publish = {
  configure: require('./lib/configure'),
  sync: require('./sync'),

  _,
  Backbone
};
publish._.extend(BackboneSQL, publish);

// re-expose modules
BackboneSQL.modules = {'backbone-orm': BackboneORM};
for (let key in BackboneORM.modules) { const value = BackboneORM.modules[key]; BackboneSQL.modules[key] = value; }
