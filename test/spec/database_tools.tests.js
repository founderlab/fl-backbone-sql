/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
const util = require('util')
const assert = require('assert')

const BackboneORM = require('backbone-orm')
const {_, Backbone, Queue, Utils} = BackboneORM

_.each(BackboneORM.TestUtils.optionSets(), (options => {
  if (typeof __test__parameters !== 'undefined' && __test__parameters !== null) { options = _.extend({}, options, __test__parameters) }
  if (options.embed) { return }

  const DATABASE_URL = options.database_url || ''
  const BASE_SCHEMA = options.schema || {}
  const SYNC = options.sync

  return describe(`Sql db tools ${options.$parameter_tags || ''}${options.$tags}`, function() {
    let Owner, Reverse
    let Flat = (Reverse = (Owner = null))
    before(function() {
      BackboneORM.configure({model_cache: {enabled: !!options.cache, max: 100}})

      Flat = class Flat extends Backbone.Model {
        static initClass() {
          this.prototype.urlRoot = `${DATABASE_URL}/flats`
          this.prototype.schema = _.extend(BASE_SCHEMA,
            {a_string: 'String'})
          this.prototype.sync = SYNC(Flat)
        }
      }
      Flat.initClass()

      Reverse = class Reverse extends Backbone.Model {
        static initClass() {
          this.prototype.urlRoot = `${DATABASE_URL}/reverses`
          this.prototype.schema = _.defaults({
            owner() { return ['belongsTo', Owner] },
            another_owner() { return ['belongsTo', Owner, {as: 'more_reverses'}] },
            many_owners() { return ['hasMany', Owner, {as: 'many_reverses'}] },
          }, BASE_SCHEMA)
          this.prototype.sync = SYNC(Reverse)
        }
      }
      Reverse.initClass()

      return Owner = (function() {
        Owner = class Owner extends Backbone.Model {
          static initClass() {
            this.prototype.urlRoot = `${DATABASE_URL}/owners`
            this.prototype.schema = _.defaults({
              a_string: 'String',
              flats() { return ['hasMany', Flat] },
              reverses() { return ['hasMany', Reverse] },
              more_reverses() { return ['hasMany', Reverse, {as: 'another_owner'}] },
              many_reverses() { return ['hasMany', Reverse, {as: 'many_owners'}] },
            }, BASE_SCHEMA)
            this.prototype.sync = SYNC(Owner)
          }
        }
        Owner.initClass()
        return Owner
      })()
    })

    after(callback => Utils.resetSchemas([Flat], callback))
    beforeEach(callback => {
      const queue = new Queue(1)
      queue.defer(callback => Utils.resetSchemas([Flat], callback))
      for (const model_type of [Flat, Reverse, Owner]) {
        (model_type => queue.defer(callback => model_type.db().dropTableIfExists(callback)))(model_type)
      }
      return queue.await(callback)
    })

    it.skip('Can drop a models table', function(done) {
      const db = Flat.db()
      return db.resetSchema(function(err) {
        assert.ok(!err, `No errors: ${err}`)
        return db.dropTable(function(err) {
          assert.ok(!err, `No errors: ${err}`)
          return db.hasTable(function(err, has_table) {
            assert.ok(!err, `No errors: ${err}`)
            assert.ok(!has_table, `Table removed: ${has_table}`)
            return done()
          })
        })
      })
    })

    it.skip('Can reset a models schema', function(done) {
      const db = Flat.db()
      return db.dropTableIfExists(function(err) {
        assert.ok(!err, `No errors: ${err}`)
        return db.resetSchema(function(err) {
          assert.ok(!err, `No errors: ${err}`)
          return db.hasColumn('a_string', function(err, has_column) {
            assert.ok(!err, `No errors: ${err}`)
            assert.ok(has_column, `Has the test column: ${has_column}`)
            return done()
          })
        })
      })
    })

    it.skip('Can ensure a models schema', function(done) {
      const db = Flat.db()
      return db.dropTableIfExists(function(err) {
        assert.ok(!err, `No errors: ${err}`)
        return db.ensureSchema(function(err) {
          assert.ok(!err, `No errors: ${err}`)
          return db.hasColumn('a_string', function(err, has_column) {
            assert.ok(!err, `No errors: ${err}`)
            assert.ok(has_column, `Has the test column: ${has_column}`)
            return done()
          })
        })
      })
    })

    it.skip('Can add a column to the db', function(done) {
      const db = Flat.db()
      return db.createTable().addColumn('test_column', 'string').end(function(err) {
        assert.ok(!err, `No errors: ${err}`)
        return db.hasColumn('test_column', function(err, has_column) {
          assert.ok(!err, `No errors: ${err}`)
          assert.ok(has_column, `Has the test column: ${has_column}`)
          return done()
        })
      })
    })

    it.skip('Can reset a single relation', function(done) {
      console.log('TODO')
      return done()
    })

    return it('Can ensure many to many models schemas', function(done) {
      const reverse_db = Reverse.db()
      const owner_db = Owner.db()

      const drop_queue = new Queue(1)

      drop_queue.defer(callback =>
        reverse_db.dropTableIfExists(function(err) {
          assert.ok(!err, `No errors: ${err}`)
          return callback()
        })
      )

      drop_queue.defer(callback =>
        owner_db.dropTableIfExists(function(err) {
          assert.ok(!err, `No errors: ${err}`)
          return callback()
        })
      )

      return drop_queue.await(function(err) {
        assert.ok(!err, `No errors: ${err}`)

        const queue = new Queue(1)

        queue.defer(callback =>
          reverse_db.ensureSchema(function(err) {
            assert.ok(!err, `No errors: ${err}`)
            return callback()
          })
        )

        queue.defer(callback =>
          owner_db.ensureSchema(function(err) {
            assert.ok(!err, `No errors: ${err}`)
            return callback()
          })
        )

        return queue.await(function(err) {
          assert.ok(!err, `No errors: ${err}`)
          return owner_db.hasColumn('a_string', function(err, has_column) {
            assert.ok(!err, `No errors: ${err}`)
            assert.ok(has_column, `Has the test column: ${has_column}`)
            return done()
          })
        })
      })
    })
  })
})
)
