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
  const SLAVE_DATABASE_URL = `${DATABASE_URL}_slave`
  const BASE_SCHEMA = options.schema || {}
  const SYNC = options.sync

  return describe(`Sql master slave selection ${options.$parameter_tags || ''}${options.$tags}`, function() {
    let Flat = null
    before(function() {
      BackboneORM.configure({model_cache: {enabled: !!options.cache, max: 100}})

      return Flat = (function() {
        Flat = class Flat extends Backbone.Model {
          static initClass() {
            this.prototype.urlRoot = `${DATABASE_URL}/flats`
            this.prototype.schema = _.extend(BASE_SCHEMA,
              {a_string: 'String'})
            this.prototype.sync = SYNC(Flat, {slaves: [SLAVE_DATABASE_URL]})
          }
        }
        Flat.initClass()
        return Flat
      })()
    })

    after(callback => Utils.resetSchemas([Flat], callback))
    beforeEach(callback => Utils.resetSchemas([Flat], callback))

    // TODO: This is wrong, maybe a way to force read from slave is needed
    return it.skip('Writes to the master database', function(done) {
      const flat = new Flat({a_string: 'hello'})
      return flat.save(function(err, saved) {
        assert.ok(!err, `No errors: ${err}`)

        return Flat.findOne(function(err, shouldnt_exist) {
          assert.ok(!err, `No errors: ${err}`)
          assert.ok(!shouldnt_exist, 'Read from slave database (model not found)')

          return done()
        })
      })
    })
  })
})
)
