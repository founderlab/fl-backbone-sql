Please refer to the following release notes when upgrading your version of BackboneSQL.

### 2.5.0
  - Fixed a bug with $unique which caused incorrect sql.

### 2.4.3
  - Fixed a bug which caused queries of the form {$or: [{'jsonField.name': value}, ...]} to be created with 'and' instead of 'or'.

### 2.2.0
  - Queries on related models no longer require a full join, so $limit and $offset work (thanks @kenhg!)

### 2.1.5
  - Accept related fields in $select (provided they are included either with $include or a query on that relation) ({$select: ['id', 'users.profile_id'})

### 2.1.4
  - Add support for related json arrays ({'relation.jsonField': 'something'})

### 2.1.3
  - Fixed a bug with $include

### 2.1.2
  - Use full table.column names for sort fields
  - Use reverse_model_type to get related model in case reverse_relation isnt present

### 2.1.0
  - Handle queries spanning multiple relations, e.g. {'profiles.users.email': 'smth@example.com'}
  - A join will be made on each relation in the query chain.

### 2.0.0
  - Potentially breaking change: All ids are now parsed into strings by default. 
    - This means that all ids, whether they came from backbone-sql or backbone-mongo, 
      will be of the same type.

### 1.3.1
  - Support $in for queries on jsonb arrays

### 1.3.0
  - Basic support for json queries in arrays

### 0.6.5
* Bug fix for missing callback
* Bug fix $exists checks for nulls
* improve row counts for $unique

### 0.6.4
* Bug fix for join tables

### 0.6.3
* Bug fix for patch remove

### 0.6.2
* Added dynamic and manual_ids capabilities

### 0.6.1
* Added unique capability

### 0.6.0
* Upgraded to BackboneORM 0.6.x

### 0.5.10
* Simplified database_tools and made compatible with the latest knex.

### 0.5.9
* Update knex due to bluebird dependency breaking.

### 0.5.8
* Fix for $ne: null in find queries

### 0.5.7
* Compatability fix for Backbone 1.1.1

### 0.5.6
* Knex bug fix for count
* Lock Backbone.js to 1.1.0 until new release compatibility issues fixed

### 0.5.5
* Updated to latest Knex (still outstanding problems with consistent Date support in Knex - not all mysql sqlite tests passing for dates)

### 0.5.4
* $nin bug fix

### 0.5.3
* $nin support

### 0.5.2
* Handle null hasMany relations in _joinedResultsToJSON

### 0.5.1
* db.ensureSchema not complain when running lots of operations

### 0.5.0
* Initial release
