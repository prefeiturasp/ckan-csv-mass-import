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
      api_key : 'cc68973f-53e6-4886-818b-ebe35f6284fa'
    };
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

Importer.prototype.call = function (options, params, type) {
    rest.postJson(this.options.host + options.path, params, {
        headers : {'Authorization': this.options.api_key},
        method  : options.method
    }).on('complete', function(data, response) {
      if (response.statusCode == 200) {
        console.log('Sucesso:', type)
      }
    }).on('fail', function(data, response){
        console.log('Falhou...', data.error, type);
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
        group        : {method: 'post', path: 'group_create'},
        // http://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.create.organization_create
        organization : {method: 'post', path: 'organization_create'},
        // http://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.create.package_create
        dataset      : {method: 'post', path: 'package_create'}
    };

_.each(files, function (v,k) {
    var input = i.read(v);
    var parsed = i.parse(input);
    _.each(parsed, function (row) {
        row.name = S(row.title).slugify().s;
        row.name = S(row.name).truncate(100).s;

        if (k === 'dataset') {
            delete row['groups']; // working in progress
            delete row['tags']; // working in progress

            var extra = [
                { 'key': 'Extensão geográfica', 'value': row['Extensão geográfica']},
                { 'key': 'Série histórica', 'value': row['Série histórica']},
                { 'key': 'Tipo de armazenamento', 'value': row['Tipo de armazenamento']},
                { 'key': 'Formato do arquivo', 'value': row['Formato do arquivo']},
                { 'key': 'Sistema Gerenciador de Banco de Dados', 'value': row['Sistema Gerenciador de Banco de Dados']},
                { 'key': 'Ambiente de produção', 'value': row['Ambiente de produção']},
                { 'key': 'Histórico', 'value': row['Histórico']},
                { 'key': 'Procedência de dados', 'value': row['Procedência de dados']},
                { 'key': 'Periodicidade', 'value': row['Periodicidade']},
                { 'key': 'Situação do processo', 'value': row['Situação do processo']},
                { 'key': 'Nível de desagregação', 'value': row['Nível de desagregação']},
                { 'key': 'Lacunas identificadas pela unidade produtora', 'value': row['Lacunas identificadas pela unidade produtora']},
                { 'key': 'Tipo(s) de representação', 'value': row['Tipo(s) de representação']},
                { 'key': 'Escala (apenas  para representação vetorial ou matricial)', 'value': row['Escala (apenas  para representação vetorial ou matricial)']},
                { 'key': 'Sistemas de referência / projeção cartográfica', 'value': row['Sistemas de referência / projeção cartográfica']},
                { 'key': 'Base cartográfica utilizada', 'value': row['Base cartográfica utilizada']},
                { 'key': 'Extensão do arquivo/formato/software', 'value': row['Extensão do arquivo/formato/software']},
                { 'key': 'Existe(m), nesta base de dados, informação(ões) classificada(s) em algum grau de sigilo?', 'value': row['Existe(m), nesta base de dados, informação(ões) classificada(s) em algum grau de sigilo?']},
                { 'key': 'Forma(s) de disponibilização', 'value': row['Forma(s) de disponibilização']},
                { 'key': 'Endereço e procedimentos para acesso', 'value': row['Endereço e procedimentos para acesso']}
            ];
            row.extras = extra;
        }

        delete row['id'];

        i.call(requests[k], row, k);
    })
});