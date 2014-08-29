/*!
 * ckan-csv-mass-import
 * https://github.com/prefeiturasp/ckan-csv-mass-import
 *
 * Released under the GPL v3
 */
/*jshint onevar: false, indent:4 */
/*global setImmediate: false, setTimeout: false, console: false */

var async  = require('async');
    csv    = require('csv'),
    fs     = require('fs'),
    path   = require('path'),
    http   = require('http'),
    rest   = require('restler'),
    _      = require('underscore'),
    S      = require('string');

Importer = function  () {
    this.options = {
      host    : 'http://cmbd.local/api/3/action/',
      api_key : '502c6e4b-3384-4ba5-b396-501ba0615324'
    };
};

Importer.prototype.validate = function (err, data) {
    if (!err) {
        return data;
    } else {
        console.log(err);
    }
};

Importer.prototype.read = function (file) {
    return fs.readFileSync(file, {encoding: 'utf-8'});
};

Importer.prototype.parse = function (raw) {
    var output = [];
    // Create the parser
    var parser = csv.parse({columns: true});
    // Use the writable stream api
    parser.on('readable', function(){
      while(record = parser.read()){
        output.push(record);
      }
    });
    // Catch any error
    parser.on('error', function(err){
      console.log(err.message);
    });

    // Now that setup is done, write data to the stream
    parser.write(raw);
    // Close the readable stream
    parser.end();
    return output;
};

Importer.prototype.call = function (options, params) {
    rest.postJson(this.options.host + options.path, params, {
        headers : {'Authorization': this.options.api_key},
        method  : options.method
    }).on('complete', function(data, response) {
        //console.log(response);
      if (response.statusCode == 201) {
        // you can get at the raw response like this...
      }
    }).on('fail', function(data, response){
        //console.log(data);
    });
};

var i = new Importer,
    files = {
        group        : path.join(__dirname, '/csv/groups.csv'),
        organization : path.join(__dirname, '/csv/organizations.csv'),
        dataset      : path.join(__dirname, '/csv/datasets.csv')
    },
    requests = {
        // http://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.create.group_create
        group        : {method: 'post', path: 'package_create'},
        // http://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.create.organization_create
        organization : {method: 'post', path: 'organization_create'},
        // http://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.create.package_create
        dataset      : {method: 'post', path: 'package_create'}
    };

_.each(files, function (v,k) {
    var input = i.read(v);
    var parsed = i.parse(input);
    _.each(parsed, function (row) {
        //console.log(row);
        row.name = S(row.title).slugify().s;
        row.name = S(row.name).truncate(100).s;
        i.call(requests[k], row);
    })
});