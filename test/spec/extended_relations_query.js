/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
let exports;
var assert = assert || (typeof require === 'function' ? require('chai').assert : undefined);

let BackboneORM = typeof window !== 'undefined' && window !== null ? window.BackboneORM : undefined; try { if (!BackboneORM) { BackboneORM = typeof require === 'function' ? require('backbone-orm') : undefined; } } catch (error) { } try { if (!BackboneORM) { BackboneORM = typeof require === 'function' ? require('../../../../backbone-orm') : undefined; } } catch (error1) {}
const {_, Backbone, Queue, Utils, JSONUtils, Fabricator} = BackboneORM;

_.each(BackboneORM.TestUtils.optionSets(), (exports = function(options) {
  if (typeof __test__parameters !== 'undefined' && __test__parameters !== null) { options = _.extend({}, options, __test__parameters); }
  if (options.embed && !options.sync.capabilities(options.database_url || '').embed) { return; }

  const DATABASE_URL = options.database_url || '';
  const BASE_SCHEMA = options.schema || {};
  const SYNC = options.sync;
  const BASE_COUNT = 20;

  const PICK_KEYS = ['id', 'name'];

  return describe(`hasMany ${options.$parameter_tags || ''}${options.$tags}`, function() {
    let Final, Owner, Reverse;
    let Flat = (Reverse = (Final = (Owner = null)));
    before(function() {
      BackboneORM.configure({model_cache: {enabled: !!options.cache, max: 100}});

      Flat = class Flat extends Backbone.Model {
        static initClass() {
          this.prototype.urlRoot = `${DATABASE_URL}/flats`;
          this.prototype.schema = BASE_SCHEMA;
          this.prototype.sync = SYNC(Flat);
        }
      };
      Flat.initClass();

      Reverse = class Reverse extends Backbone.Model {
        static initClass() {
          this.prototype.urlRoot = `${DATABASE_URL}/reverses`;
          this.prototype.schema = _.defaults({
            owner() { return ['belongsTo', Owner]; },
            another_owner() { return ['belongsTo', Owner, {as: 'more_reverses'}]; },
            finals() { return ['hasMany', Final]; }
          }, BASE_SCHEMA);
          this.prototype.sync = SYNC(Reverse);
        }
      };
      Reverse.initClass();

      Final = class Final extends Backbone.Model {
        static initClass() {
          this.prototype.urlRoot = `${DATABASE_URL}/finals`;
          this.prototype.schema = _.defaults({
            reverse() { return ['belongsTo', Reverse]; }
          }, BASE_SCHEMA);
          this.prototype.sync = SYNC(Final);
        }
      };
      Final.initClass();

      return Owner = (function() {
        Owner = class Owner extends Backbone.Model {
          static initClass() {
            this.prototype.urlRoot = `${DATABASE_URL}/owners`;
            this.prototype.schema = _.defaults({
              flats() { return ['hasMany', Flat]; },
              reverses() { return ['hasMany', Reverse]; },
              more_reverses() { return ['hasMany', Reverse, {as: 'another_owner'}]; }
            }, BASE_SCHEMA);
            this.prototype.sync = SYNC(Owner);
          }
        };
        Owner.initClass();
        return Owner;
      })();
    });

    after(callback => Utils.resetSchemas([Flat, Reverse, Final, Owner], callback));

    beforeEach(function(callback) {
      const relation = Owner.relation('reverses');
      delete relation.virtual;
      const MODELS = {};

      const queue = new Queue(1);
      queue.defer(callback => Utils.resetSchemas([Flat, Reverse, Final, Owner], callback));
      queue.defer(function(callback) {
        const create_queue = new Queue();

        create_queue.defer(callback => Fabricator.create(Flat, 20*BASE_COUNT, {
          name: Fabricator.uniqueId('flat_'),
          created_at: Fabricator.date
        }, function(err, models) { MODELS.flat = models; return callback(err); })
         );
        create_queue.defer(callback => Fabricator.create(Reverse, 2*BASE_COUNT, {
          name: Fabricator.uniqueId('reverse_'),
          created_at: Fabricator.date
        }, function(err, models) { MODELS.reverse = models; return callback(err); })
         );
        create_queue.defer(callback => Fabricator.create(Reverse, 2*BASE_COUNT, {
          name: Fabricator.uniqueId('reverse_'),
          created_at: Fabricator.date
        }, function(err, models) { MODELS.more_reverse = models; return callback(err); })
         );
        create_queue.defer(callback => Fabricator.create(Final, 2*BASE_COUNT, {
          name: Fabricator.uniqueId('final_'),
          created_at: Fabricator.date
        }, function(err, models) { MODELS.final = models; return callback(err); })
         );
        create_queue.defer(callback => Fabricator.create(Owner, BASE_COUNT, {
          name: Fabricator.uniqueId('owner_'),
          created_at: Fabricator.date
        }, function(err, models) { MODELS.owner = models; return callback(err); })
         );

        return create_queue.await(callback);
      });

      // link and save all
      queue.defer(function(callback) {
        let link_task;
        const save_queue = new Queue();

        const link_tasks = [];
        for (let owner of Array.from(MODELS.owner)) {
          link_task = {
            owner,
            values: {
              flats: [MODELS.flat.pop(), MODELS.flat.pop()],
              reverses: [MODELS.reverse.pop(), MODELS.reverse.pop()],
              more_reverses: [MODELS.more_reverse.pop(), MODELS.more_reverse.pop()]
            },
            secondary_values: {
              finals: [MODELS.final.pop(), MODELS.final.pop()]
            }
          };
          link_tasks.push(link_task);
        }

        for (link_task of Array.from(link_tasks)) { (link_task => save_queue.defer(function(callback) {
          const q = new Queue();
          _.forEach(link_task.values.reverses, reverse => q.defer(function(callback) {
            reverse.set(link_task.secondary_values);
            return reverse.save(callback);
          })
           );
          return q.await(function() {
            link_task.owner.set(link_task.values);
            return link_task.owner.save(callback);
          });
        }) )(link_task); }

        return save_queue.await(callback);
      });

      return queue.await(callback);
    });

    it('Can query simple relationships (hasMany)', done =>
      Final.findOne(function(err, final) {
        assert.ok(!err, `No errors: ${err}`);
        const query = {
          'finals.id': final.id,
          $select: 'id',
          $verbose: true,
        };
        return Reverse.cursor(query).toJSON(function(err, reverse) {
          assert.ok(!err, `No errors: ${err}`);
          assert.ok(reverse, 'found model');
          return done();
        });
      })
    );

    it('Can query simple relationships (belongsTo)', done =>
      Reverse.findOne(function(err, reverse) {
        assert.ok(!err, `No errors: ${err}`);
        const query = {
          'reverse.name': reverse.get('name'),
          $select: 'id',
          $verbose: true,
        };
        return Final.cursor(query).toJSON(function(err, reverse) {
          assert.ok(!err, `No errors: ${err}`);
          assert.ok(reverse, 'found model');
          return done();
        });
      })
    );

    it('Can query extended relationships', done =>
      Final.findOne(function(err, final) {
        assert.ok(!err, `No errors: ${err}`);
        const query = {
          'reverses.finals.id': final.id,
          $verbose: true,
        };
        return Owner.cursor(query).toJSON(function(err, owners) {
          assert.ok(!err, `No errors: ${err}`);
          assert.ok(owners.length, 'found models');

          return Reverse.cursor({'finals.id': final.id}).toJSON(function(err, reverses) {
            assert.ok(!err, `No errors: ${err}`);
            assert.ok(reverses, 'found models');

            _.forEach(reverses, reverse =>
              _.forEach(owners, owner => assert.equal(reverse.owner_id, owner.id))
            );

            return done();
          });
        });
      })
    );

    it('Can query extended relationships', done =>
      Final.findOne(function(err, final) {
        assert.ok(!err, `No errors: ${err}`);
        const query = {
          'reverses.finals.id': final.id,
          $verbose: true,
          $include: 'reverses',
        };
        return Owner.cursor(query).toJSON(function(err, owners) {
          assert.ok(!err, `No errors: ${err}`);
          assert.ok(owners.length, 'found models');

          return Reverse.cursor({'finals.id': final.id}).toJSON(function(err, reverses) {
            assert.ok(!err, `No errors: ${err}`);
            assert.ok(reverses, 'found models');

            _.forEach(reverses, reverse =>
              _.forEach(owners, owner => assert.equal(reverse.owner_id, owner.id))
            );

            return done();
          });
        });
      })
    );

    it('Can query extended relationships with paging', done =>
      Final.findOne(function(err, final) {
        assert.ok(!err, `No errors: ${err}`);
        const query = {
          'reverses.finals.id': final.id,
          $verbose: true,
          $page: true,
        };
        return Owner.cursor(query).toJSON(function(err, paging_info) {
          assert.ok(!err, `No errors: ${err}`);
          assert.equal(0, paging_info.offset, `Has offset. Expected: 0. Actual: ${paging_info.offset}`);
          assert.equal(1, paging_info.total_rows, `Counted owners. Expected: 1. Actual: ${paging_info.total_rows}`);
          return done();
        });
      })
    );

    return it('Can query extended relationships with limit', function(done) {
      const limit = 5;
      return Final.findOne(function(err, final) {
        assert.ok(!err, `No errors: ${err}`);
        const query = {
          'reverses.finals.id': final.id,
          $verbose: true,
          $limit: limit,
        };
        return Owner.cursor(query).toJSON(function(err, owners) {
          assert.ok(!err, `No errors: ${err}`);
          assert.equal(owners.length, 5, 'found models');

          return Reverse.cursor({'finals.id': final.id}).toJSON(function(err, reverses) {
            assert.ok(!err, `No errors: ${err}`);
            assert.ok(reverses, 'found models');

            _.forEach(reverses, reverse =>
              _.forEach(owners, owner => assert.equal(reverse.owner_id, owner.id))
            );

            return done();
          });
        });
      });
    });
  });
})
);
