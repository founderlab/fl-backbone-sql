/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
let buildLibraries
const _ = require('underscore')
const es = require('event-stream')

const Async = require('async')
const gulp = require('gulp')
const gutil = require('gulp-util')
const mocha = require('gulp-spawn-mocha')

const MOCHA_DATABASE_OPTIONS =
  {postgres: {require: ['test/parameters_postgres'], env: {NODE_ENV: 'test'}}}
  // mysql: {require: ['test/parameters_mysql'], env: {NODE_ENV: 'test'}}
  // sqlite3: {require: ['test/parameters_sqlite3'], env: {NODE_ENV: 'test'}}

const testFn = (options={}) => callback => {
  const tags = (Array.from(process.argv.slice(3)).map((tag) => `@${tag.replace(/^[-]+/, '')}`)).join(' ')
  gutil.log(`Running tests for ${options.protocol} ${tags}`)

  gulp.src([
    'node_modules/backbone-orm/test/{issues,spec/sync}/**/*.tests.coffee',
      // "node_modules/backbone-orm/test/spec/sync/relational/has_many.tests.coffee"
  ])
    .pipe(mocha(_.extend({reporter: 'dot', grep: tags}, MOCHA_DATABASE_OPTIONS[options.protocol])))
    .pipe(es.writeArray(callback))
   // promises workaround: https://github.com/gulpjs/gulp/issues/455
}

gulp.task('test', function(callback) {
  Async.series(((() => {
    const result = []
    for (const protocol in MOCHA_DATABASE_OPTIONS) {
      result.push(testFn({protocol}))
    }
    return result
  })()), callback)
}) // promises workaround: https://github.com/gulpjs/gulp/issues/455

gulp.task('test-postgres', testFn({protocol: 'postgres'}))
// gulp.task 'test-mysql', ['build'], testFn({protocol: 'mysql'})
// gulp.task 'test-sqlite3', ['build'], testFn({protocol: 'sqlite3'})
