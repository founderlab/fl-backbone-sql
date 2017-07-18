/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
let buildLibraries;
const _ = require('underscore');
const es = require('event-stream');

const Async = require('async');
const gulp = require('gulp');
const gutil = require('gulp-util');
const coffee = require('gulp-coffee');
const mocha = require('gulp-spawn-mocha');

gulp.task('build', (buildLibraries = () =>
  gulp.src('./src/**/*.coffee')
    .pipe(coffee({header: true})).on('error', gutil.log)
    .pipe(gulp.dest('./lib'))
)
);

gulp.task('watch', ['build'], () => gulp.watch('./src/**/*.coffee', () => buildLibraries()));

const MOCHA_DATABASE_OPTIONS =
  {postgres: {require: ['test/parameters_postgres', 'backbone-rest/test/parameters_express4'], env: {NODE_ENV: 'test'}}};
  // mysql: {require: ['test/parameters_mysql', 'backbone-rest/test/parameters_express4'], env: {NODE_ENV: 'test'}}
  // sqlite3: {require: ['test/parameters_sqlite3', 'backbone-rest/test/parameters_express4'], env: {NODE_ENV: 'test'}}

const testFn = function(options) { if (options == null) { options = {}; } return function(callback) {
  const tags = (Array.from(process.argv.slice(3)).map((tag) => `@${tag.replace(/^[-]+/, '')}`)).join(' ');
  gutil.log(`Running tests for ${options.protocol} ${tags}`);

  gulp.src([
      // "test/spec/extended_relations_query.coffee"
      "node_modules/backbone-orm/test/{issues,spec/sync}/**/*.tests.coffee",
      // "node_modules/backbone-orm/test/spec/sync/relational/has_many.tests.coffee"
      `${tags.indexOf('@quick') >= 0 ? '' : '{node_modules/backbone-rest/,}'}test/spec/**/*.tests.coffee`
    ])
    .pipe(mocha(_.extend({reporter: 'dot', grep: tags}, MOCHA_DATABASE_OPTIONS[options.protocol])))
    .pipe(es.writeArray(callback));
   // promises workaround: https://github.com/gulpjs/gulp/issues/455
}; };

gulp.task('test', ['build'], function(callback) {
  Async.series(((() => {
    const result = [];
    for (let protocol in MOCHA_DATABASE_OPTIONS) {
      result.push(testFn({protocol}));
    }
    return result;
  })()), callback);
}); // promises workaround: https://github.com/gulpjs/gulp/issues/455
gulp.task('test-postgres', ['build'], testFn({protocol: 'postgres'}));
// gulp.task 'test-mysql', ['build'], testFn({protocol: 'mysql'})
// gulp.task 'test-sqlite3', ['build'], testFn({protocol: 'sqlite3'})

// gulp.task 'benchmark', ['build'], (callback) ->
//   (require './test/lib/run_benchmarks')(callback)
//   return # promises workaround: https://github.com/gulpjs/gulp/issues/455
