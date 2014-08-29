/*!
 * async
 * https://github.com/caolan/async
 *
 * Copyright 2010-2014 Caolan McMahon
 * Released under the MIT license
 */
/*jshint onevar: false, indent:4 */
/*global setImmediate: false, setTimeout: false, console: false */

var async  = require('async');
    csv    = require('csv'),
    fs     = require('fs'),
    path   = require('path'),
    http   = require('http'),
    rest   = require('restler'),
    _      = require('underscore');

Importer = function  () {
    this.data = {};
    this.options = {
      host    : 'http://cmbd.local/api/3/',
      api_key : '502c6e4b-3384-4ba5-b396-501ba0615324'
    };
    this.files = {
        group        : path.join(__dirname, '/csv/groups.csv'),
        organization : path.join(__dirname, '/csv/organizations.csv'),
        dataset      : path.join(__dirname, '/csv/datasets.csv')
    };
    this.requests = {
        // http://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.create.group_create
        group        : {method: 'POST', path: 'package_create'},
        // http://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.create.organization_create
        organization : {method: 'POST', path: 'organization_create'},
        // http://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.create.package_create
        dataset      : {method: 'POST', path: 'package_create'}
    };
};

Importer.prototype.validateData = function (err, data) {
    if (!err) {
        return data;
    } else {
        console.log(err);
    }
};

Importer.prototype.getFile = function (filePath) {
    return fs.readFileSync(filePath, {encoding: 'utf-8'});
};

Importer.prototype.parseData = function (raw_data) {
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
    parser.write(raw_data);
    // Close the readable stream
    parser.end();
    return output;
};

Importer.prototype.save = function (resource, data) {
    rest.post('http://user:pass@service.com/action', {
      data: { id: 334 },
    }).on('complete', function(data, response) {
      if (response.statusCode == 201) {
        // you can get at the raw response like this...
      }
    });
};

var i = new Importer;

_.each(i.files, function (v,k) {
    var input = i.getFile(v);
    var parsed = i.parseData(input);
console.log(parsed);
});




