assert = assert or require?('chai').assert

BackboneORM = window?.BackboneORM; try BackboneORM or= require?('backbone-orm') catch; try BackboneORM or= require?('../../../../backbone-orm')
{_, Backbone, Queue, Utils, JSONUtils, Fabricator} = BackboneORM

_.each BackboneORM.TestUtils.optionSets()[0..0], exports = (options) ->
  options = _.extend({}, options, __test__parameters) if __test__parameters?
  return if options.embed and not options.sync.capabilities(options.database_url or '').embed

  DATABASE_URL = options.database_url or ''
  BASE_SCHEMA = options.schema or {}
  SYNC = options.sync
  BASE_COUNT = 5

  PICK_KEYS = ['id', 'name']

  describe "hasMany #{options.$parameter_tags or ''}#{options.$tags}", ->
    Flat = Reverse = Final = Owner = null
    before ->
      BackboneORM.configure {model_cache: {enabled: !!options.cache, max: 100}}

      class Flat extends Backbone.Model
        urlRoot: "#{DATABASE_URL}/flats"
        schema: BASE_SCHEMA
        sync: SYNC(Flat)

      class Reverse extends Backbone.Model
        urlRoot: "#{DATABASE_URL}/reverses"
        schema: _.defaults({
          owner: -> ['belongsTo', Owner]
          another_owner: -> ['belongsTo', Owner, as: 'more_reverses']
          finals: -> ['hasMany', Final]
        }, BASE_SCHEMA)
        sync: SYNC(Reverse)

      class Final extends Backbone.Model
        urlRoot: "#{DATABASE_URL}/finals"
        schema: _.defaults({
          reverse: -> ['belongsTo', Reverse]
        }, BASE_SCHEMA)
        sync: SYNC(Final)

      class Owner extends Backbone.Model
        urlRoot: "#{DATABASE_URL}/owners"
        schema: _.defaults({
          flats: -> ['hasMany', Flat]
          reverses: -> ['hasMany', Reverse]
          more_reverses: -> ['hasMany', Reverse, as: 'another_owner']
        }, BASE_SCHEMA)
        sync: SYNC(Owner)

    after (callback) -> Utils.resetSchemas [Flat, Reverse, Final, Owner], callback

    beforeEach (callback) ->
      relation = Owner.relation('reverses')
      delete relation.virtual
      MODELS = {}

      queue = new Queue(1)
      queue.defer (callback) -> Utils.resetSchemas [Flat, Reverse, Final, Owner], callback
      queue.defer (callback) ->
        create_queue = new Queue()

        create_queue.defer (callback) -> Fabricator.create Flat, 20*BASE_COUNT, {
          name: Fabricator.uniqueId('flat_')
          created_at: Fabricator.date
        }, (err, models) -> MODELS.flat = models; callback(err)
        create_queue.defer (callback) -> Fabricator.create Reverse, 2*BASE_COUNT, {
          name: Fabricator.uniqueId('reverse_')
          created_at: Fabricator.date
        }, (err, models) -> MODELS.reverse = models; callback(err)
        create_queue.defer (callback) -> Fabricator.create Reverse, 2*BASE_COUNT, {
          name: Fabricator.uniqueId('reverse_')
          created_at: Fabricator.date
        }, (err, models) -> MODELS.more_reverse = models; callback(err)
        create_queue.defer (callback) -> Fabricator.create Final, BASE_COUNT, {
          name: Fabricator.uniqueId('final_')
          created_at: Fabricator.date
        }, (err, models) -> MODELS.final = models; callback(err)
        create_queue.defer (callback) -> Fabricator.create Owner, BASE_COUNT, {
          name: Fabricator.uniqueId('owner_')
          created_at: Fabricator.date
        }, (err, models) -> MODELS.owner = models; callback(err)

        create_queue.await callback

      # link and save all
      queue.defer (callback) ->
        save_queue = new Queue()

        link_tasks = []
        for owner in MODELS.owner
          link_task =
            owner: owner
            values:
              flats: [MODELS.flat.pop(), MODELS.flat.pop()]
              reverses: [MODELS.reverse.pop(), MODELS.reverse.pop()]
              more_reverses: [MODELS.more_reverse.pop(), MODELS.more_reverse.pop()]
            secondary_values:
              finals: [MODELS.final.pop()]
          link_tasks.push(link_task)

        for link_task in link_tasks then do (link_task) -> save_queue.defer (callback) ->
          reverse = link_task.values.reverses[0]
          reverse.set(link_task.secondary_values)
          reverse.save ->
            link_task.owner.set(link_task.values)
            link_task.owner.save callback

        save_queue.await callback

      queue.await callback

    # it 'Can query simple relationships', (done) ->
    #   Final.findOne (err, final) ->
    #     assert.ok(!err, "No errors: #{err}")
    #     query = {
    #       'finals.id': final.id,
    #       $select: 'id',
    #       $verbose: true,
    #     }
    #     Reverse.cursor(query).toJSON (err, reverse) ->
    #       assert.ok(!err, "No errors: #{err}")
    #       assert.ok(reverse, 'found model')
    #       console.dir(reverse)
    #       done()

    it 'Can query extended relationships', (done) ->
      Final.findOne (err, final) ->
        assert.ok(!err, "No errors: #{err}")
        query = {
          'reverses.finals.id': final.id,
          $select: 'id',
          $verbose: true,
          # $include: ['reverses'],
        }
        Owner.cursor(query).toJSON (err, owner) ->
          assert.ok(!err, "No errors: #{err}")
          assert.ok(owner, 'found model')
          console.dir(owner)

          # assert.equal(0, paging_info.offset, "Has offset. Expected: 0. Actual: #{paging_info.offset}")
          # assert.equal(2, paging_info.total_rows, "Counted reverses. Expected: 2. Actual: #{paging_info.total_rows}")
          done()

    # it 'Can query extended relationships with paging', (done) ->
    #   Owner.findOne (err, owner) ->
    #     assert.ok(!err, "No errors: #{err}")
    #     assert.ok(owner, 'found model')

    #     Reverse.cursor({owner_id: owner.id, $page: true}).toJSON (err, paging_info) ->
    #       assert.ok(!err, "No errors: #{err}")
    #       assert.equal(0, paging_info.offset, "Has offset. Expected: 0. Actual: #{paging_info.offset}")
    #       assert.equal(2, paging_info.total_rows, "Counted reverses. Expected: 2. Actual: #{paging_info.total_rows}")
    #       done()

    # it 'Can include related models', (done) ->
    #   Owner.cursor({$one: true}).include('flats').toJSON (err, test_model) ->
    #     assert.ok(!err, "No errors: #{err}")
    #     assert.ok(test_model, 'found model')
    #     assert.ok(test_model.flats, 'Has related flats')
    #     assert.equal(test_model.flats.length, 2, "Has the correct number of related flats \nExpected: #{2}\nActual: #{test_model.flats.length}")
    #     done()
