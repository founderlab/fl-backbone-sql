/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS103: Rewrite code to no longer use __guard__
 * DS104: Avoid inline assignments
 * DS204: Change includes calls to have a more natural evaluation order
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
/*
  backbone-sql.js 0.6.5
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-sql
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
*/
let SqlAst;
const _ = require('lodash');
const Utils = require('./lib/utils');

const COMPARATORS = {
  $lt: '<',
  $lte: '<=',
  $gt: '>',
  $gte: '>=',
  $ne: '!=',
  $eq: '='
};
const COMPARATOR_KEYS = _.keys(COMPARATORS);

module.exports = (SqlAst = class SqlAst {

  constructor(options) {
    this.select = [];
    this.where = {method: 'where', conditions: []};
    this.joins = {};
    this.sort = null;
    this.limit = null;
    if (options) { this.parse(options); }
  }

  // Public method that sets up for parsing
  parse(options) {
    this.find = options.find || {};
    this.cursor = options.cursor || {};
    this.query = options.query || _.extend({}, this.find, this.cursor);
    if (!(this.model_type = options.model_type)) { throw new Error('Ast requires a model_type option'); }

    this.prefix_columns = options.prefix_columns;

    if (this.query.$count) { this.count = true; }
    if (this.query.$exists) { this.exists = true; }
    this.limit = this.query.$limit || (this.query.$one ? 1 : null);
    this.offset = this.query.$offset;
    if (this.query.$include) {
      if (!_.isArray(this.query.$include)) { this.query.$include = [this.query.$include]; }
      this.prefix_columns = true;
      for (let key of Array.from(this.query.$include)) { this.join(key, this.getRelation(key), {include: true}); }
    }

    this.where.conditions = this.parseQuery(this.query, {table: this.model_type.tableName()});

    this.setSortFields(this.query.$sort);
    return this.setSelectedColumns();
  }

  // Internal parse method that recursively parses the query
  parseQuery(query, options) {
    let cond, q;
    if (options == null) { options = {}; }
    const { table } = options;
    if (!options.method) { options.method = 'where'; }
    const conditions = [];

    for (let key in query) {
      const value = query[key];
      if (key[0] !== '$') {var reverse_relation;
      
        if (_.isUndefined(value)) { throw new Error(`Unexpected undefined for query key '${key}'`); }

        // A dot indicates a condition on a relation model
        if (key.indexOf('.') > 0) {
          if (cond = this.parseJsonField(key, value, options)) {
            conditions.push(cond);
          } else {
            cond = this.parseDotRelation(key, value, options);
            conditions.push(cond);
          }

        // Many to Many relationships may be queried on the foreign key of the join table
        } else if ((reverse_relation = this.model_type.reverseRelation(key)) && reverse_relation.join_table) {
          let relation, relation_key;
          [cond, relation_key, relation] = Array.from(this.parseManyToManyRelation(key, value, reverse_relation));
          this.join(relation_key, relation, {pivot_only: true});
          conditions.push(cond);

        } else {
          cond = this.parseCondition(key, value, {table, method: options.method});
          conditions.push(cond);
        }
      }
    }

    if (query != null ? query.$ids : undefined) {
      cond = this.parseCondition('id', {$in: query.$ids}, {table});
      conditions.push(cond);
      if (!query.$ids.length) { this.abort = true; }
    }

    if (query != null ? query.$or : undefined) {
      const or_where = {method: options.method, conditions: []};
      for (q of Array.from(query.$or)) {
        or_where.conditions = or_where.conditions.concat(this.parseQuery(q, {table, method: 'orWhere'}));
      }
      conditions.push(or_where);
    }

    if (query != null ? query.$and : undefined) {
      const and_where = {method: options.method, conditions: []};
      for (q of Array.from(query.$and)) {
        and_where.conditions = and_where.conditions.concat(this.parseQuery(q, {table}));
      }
      conditions.push(and_where);
    }

    return conditions;
  }

  // Take a list of relation keys and create conditions for them
  // The final key is the field of the final model to query on
  //
  // keys may be of the form `reverse.final.name`
  // where `reverse` and `final` are relations
  // and `name` is the field to query on from the `final` relation
  //
  // recursively nest conditions via the `dot_where` property on the conditions
  relatedCondition(keys, value, previous_model_type, options) {
    let condition;
    const relation_key = keys.shift();
    const relation = this.getRelation(relation_key, previous_model_type);
    const model_type = relation.reverse_model_type;

    // Has further relations to process
    if (keys.length > 1) {
      condition = {
        relation,
        model_type,
        key: relation_key,
        method: 'whereIn',
        dot_where: this.relatedCondition(keys, value, model_type, options)
      };

    // No further relations to process -  the remaining key is the field to query against
    } else {
      const key = keys.pop();
      options = _.extend(options, {
        relation,
        model_type,
        table: model_type.tableName(),
        method: options.method
      });
      condition = this.parseCondition(key, value, options);
    }

    return condition;
  }

  parseDotRelation(key, value, options) {
    return this.relatedCondition(key.split('.'), value, this.model_type, options);
  }

  join(relation_key, relation, options) {
    if (options == null) { options = {}; }
    this.prefix_columns = true;
    if (!relation) { relation = this.getRelation(relation_key); }
    const model_type = relation.reverse_model_type;
    return this.joins[relation_key] = _.extend((this.joins[relation_key] || {}), {
      relation,
      key: relation_key,
      columns: Array.from(model_type.schema().columns()).map((col) => this.prefixColumn(col, model_type.tableName()))
    }, options);
  }

  isJsonField(json_field, model_type) {
    let needle;
    if (!model_type) { ({ model_type } = this); }
    const field = model_type.schema().fields[json_field];
    return field && (needle = field.type.toLowerCase(), ['json', 'jsonb'].includes(needle));
  }

  parseJsonField(key, value, options) {
    if (options == null) { options = {}; }
    const [json_field, attr] = Array.from(key.split('.'));
    if (this.isJsonField(json_field)) {
      const value_string = JSON.stringify(value);
      const cond = {
        method: options.method === 'orWhere' ? 'orWhereRaw' : 'whereRaw',
        key: `\"${json_field}\" @> ?`,
        value: `[{\"${attr}\": ${value_string}}]`
      };
      return cond;
    }

    return null;
  }

  parseManyToManyRelation(key, value, reverse_relation) {
    const relation = reverse_relation.reverse_relation;
    const relation_key = relation.key;
    const cond = this.parseCondition(reverse_relation.foreign_key, value, {relation, model_type: relation.model_type, table: relation.join_table.tableName()});
    return [cond, relation_key, relation];
  }

  parseCondition(_key, value, options) {
    if (options == null) { options = {}; }
    let method = options.method || 'where';
    const key = this.columnName(_key, options.table);

    const condition = {method, conditions: [], relation: options.relation, model_type: options.model_type};

    if (_.isObject(value) && !_.isDate(value)) {

      let mongo_conditions, val;
      if (value != null ? value.$in : undefined) {
        if (!value.$in.length) {
          this.abort = true;
          return condition;
        }
        if (this.isJsonField(_key) || (options.relation && this.isJsonField(_key, options.model_type))) {
          for (val of Array.from(value.$in)) {
            condition.conditions.push({
              method: 'orWhere',
              conditions: [{
                key: '?? \\? ?',
                value: [key, val],
                method: 'whereRaw',
                relation: options.relation,
                model_type: options.model_type
              }]
            });
          }
          return condition;
        } else {
          condition.conditions.push({key, method: 'whereIn', value: value.$in, relation: options.relation, model_type: options.model_type});
        }
      }

      if (value != null ? value.$nin : undefined) {
        condition.conditions.push({key, method: 'whereNotIn', value: value.$nin, relation: options.relation, model_type: options.model_type});
      }

      if ((value != null ? value.$exists : undefined) != null) {
        condition.conditions.push({key, method: ((value != null ? value.$exists : undefined) ? 'whereNotNull' : 'whereNull'), relation: options.relation, model_type: options.model_type});
      }

      // Transform a conditional of type {key: {$like: 'string'}} to ('key', 'like', '%string%')
      if (_.isObject(value) && value.$like) {
        val = Array.from(value.$like).includes('%') ? value.$like : `%${value.$like}%`;
        condition.conditions.push({key, method, operator: 'ilike', value: val, relation: options.relation, model_type: options.model_type});
      }

      // Transform a conditional of type {key: {$lt: 5, $gt: 3}} to [('key', '<', 5), ('key', '>', 3)]
      if (_.size(mongo_conditions = _.pick(value, COMPARATOR_KEYS))) {
        for (let mongo_op in mongo_conditions) {
          val = mongo_conditions[mongo_op];
          const operator = COMPARATORS[mongo_op];

          if (mongo_op === '$ne') {
            if (_.isNull(val)) {
              condition.conditions.push({key, method: `${method}NotNull`}, {relation: options.relation, model_type: options.model_type});
            } else {
              condition.conditions.push({method, conditions: [
                {key, operator, method: 'orWhere', value: val, relation: options.relation},
                {key, method: 'orWhereNull', relation: options.relation}
              ]});
            }

          } else if (_.isNull(val)) {
            if (mongo_op === '$eq') {
              condition.conditions.push({key, method: `${method}Null`, relation: options.relation, model_type: options.model_type});
            } else {
              throw new Error(`Unexpected null with query key '${key}': '${mongo_conditions}'`);
            }

          } else {
            condition.conditions.push({key, operator, method, value: val, relation: options.relation, model_type: options.model_type});
          }
        }
      }

    } else {
      if (this.isJsonField(_key) || (options.relation && this.isJsonField(_key, options.model_type))) {
        _.extend(condition, {
          key: '?? \\? ?',
          value: [key, value],
          method: 'whereRaw'
        });
      } else {
        if (['where', 'orWhere'].includes(method) && _.isNull(value)) { method = `${method}Null`; }
        _.extend(condition, {key, value, method});
      }
    }

    if (_.isArray(condition.conditions) && (condition.conditions.length === 1)) {
      return condition.conditions[0];
    }

    return condition;
  }

  // Set up sort columns
  setSortFields(sort) {
    if (!sort) { return; }
    this.sort = [];
    const to_sort = _.isArray(this.query.$sort) ? this.query.$sort : [this.query.$sort];
    return (() => {
      const result = [];
      for (let sort_key of Array.from(to_sort)) {
        const [column, direction] = Array.from(Utils.parseSortField(sort_key));
        if (this.prefix_columns && !Array.from(column).includes('.')) {
          result.push(this.sort.push({column: this.columnName(column, this.model_type.tableName()), direction}));
        } else {
          result.push(this.sort.push({column, direction}));
        }
      }
      return result;
    })();
  }

  // Ensure that column references have table prefixes where required
  setSelectedColumns() {
    this.columns = this.model_type.schema().columns();
    if (!Array.from(this.columns).includes('id')) { this.columns.unshift('id'); }

    if (this.query.$values) {
      this.fields = this.query.$whitelist ? _.intersection(this.query.$values, this.query.$whitelist) : this.query.$values;
    } else if (this.query.$select) {
      this.fields = this.query.$whitelist ? _.intersection(this.query.$select, this.query.$whitelist) : this.query.$select;
    } else if (this.query.$whitelist) {
      this.fields = this.query.$whitelist;
    } else {
      this.fields = this.columns;
    }

    this.select = [];
    for (let col of Array.from(this.fields)) {
      this.select.push(this.prefix_columns ? this.prefixColumn(col, this.model_type.tableName()) : col);
    }

    if (this.query.$include) {
      return Array.from(this.query.$include).map((key) =>
        (this.select = this.select.concat(this.joins[key].columns)));
    }
  }

  jsonColumnName(attr, col, table) { return `${table}->'${col}'->>'${attr}'`; }

  columnName(col, table) { return `${table}.${col}`; } //if table and @prefix_columns then "#{table}.#{col}" else col

  prefixColumn(col, table) {
    if (Array.from(col).includes('.')) { return col; }
    return `${table}.${col} as ${this.tablePrefix(table)}${col}`;
  }

  prefixColumns(cols, table) { return Array.from(cols).map((col) => this.prefixColumn(col, table)); }

  tablePrefix(table) { return `${table}_`; }

  prefixRegex(table) {
    if (!table) { table = this.model_type.tableName(); }
    return new RegExp(`^${this.tablePrefix(table)}(.*)$`);
  }

  getRelation(key, model_type) {
    let relation;
    if (!model_type) { ({ model_type } = this); }
    if (!(relation = model_type.relation(key))) { throw new Error(`${key} is not a relation of ${model_type.model_name}`); }
    return relation;
  }

  joinedIncludesWithConditions() { return (() => {
    const result = [];
    for (let key in this.joins) {
      const join = this.joins[key];
      if (join.include && join.condition) {
        result.push(join);
      }
    }
    return result;
  })(); }

  print() {
    console.log('********************** AST ******************************');

    console.log('---- Input ----');
    console.log('> query:', this.query);

    console.log();

    console.log('----  AST  ----');
    console.log('> select:', this.select);
    console.log('> where:');
    this.printCondition(this.where);
    console.log('> joins:', ((() => {
      const result = [];
      for (let key in this.joins) {
        const join = this.joins[key];
        result.push([key, `include: ${join.include}`, join.columns]);
      }
      return result;
    })()));
    console.log('> count:', this.count);
    console.log('> exists:', this.exists);
    console.log('> sort:', this.sort);
    console.log('> limit:', this.limit);

    return console.log('---------------------------------------------------------');
  }

  printCondition(cond, indent) {
    if (indent == null) { indent = ''; }
    process.stdout.write(indent);
    const to_print = _.omit(cond, 'relation', 'model_type', 'previous_model_type', 'conditions', 'dot_where');
    // console.dir(cond)

    const model_name = cond.model_type != null ? cond.model_type.model_name : undefined;
    if (model_name) { to_print.model_name = model_name; }

    const previous_model_name = __guard__(cond.relation != null ? cond.relation.model_type : undefined, x => x.model_name);
    if (previous_model_name) { to_print.previous_model_name = previous_model_name; }

    console.dir(to_print, {depth: null, colors: true});
    if (cond.conditions != null ? cond.conditions.length : undefined) {
      console.log(indent + '[');
      for (let c of Array.from(cond.conditions)) { this.printCondition(c, indent + '  '); }
      console.log(indent + ']');
    }
    if (cond.dot_where) {
      return this.printCondition(cond.dot_where, indent + '  ');
    }
  }
});

function __guard__(value, transform) {
  return (typeof value !== 'undefined' && value !== null) ? transform(value) : undefined;
}