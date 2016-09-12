// Generated by CoffeeScript 1.10.0

/*
  backbone-sql.js 0.6.5
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-sql
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
 */

(function() {
  var SqlBackboneAdapter, _;

  _ = require('backbone-orm')._;

  module.exports = SqlBackboneAdapter = (function() {
    function SqlBackboneAdapter() {}

    SqlBackboneAdapter.nativeToAttributes = function(json, schema) {
      var err, error, key, ref, ref1, value;
      ref = schema.fields;
      for (key in ref) {
        value = ref[key];
        if (schema.fields[key] && schema.fields[key].type === 'Boolean' && json[key] !== null) {
          json[key] = !!json[key];
        } else if (((ref1 = value.type) != null ? ref1.toLowerCase() : void 0) === 'json' && json[key]) {
          try {
            json[key] = JSON.parse(json[key]);
          } catch (error) {
            err = error;
          }
        }
      }
      return json;
    };

    return SqlBackboneAdapter;

  })();

}).call(this);