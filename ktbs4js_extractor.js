var async = require('async');
var crypto = require('crypto');
var mysql = require('mysql');
var requirejs = require('requirejs');

var jQuery, traceManager;

// Opens a mySQL connection to the moodle database
// TODO Read the  parameters from an external configuration file
function createMySqlConnection() {
	var connection = mysql.createConnection({
		host : 'localhost',
		database : 'madoc',
		user : 'root',
		password : 'moodle'
	});

	return connection;
}

// Creates a sha hash from parameter toHash
function shaHash(toHash) {
	var shaSum = crypto.createHash('sha1');
	shaSum.update(toHash);
	var digest = shaSum.digest('hex');
	return digest;
}

function extractLog(row, callback) {
	var username;
	if (row.username === 'guest') {
		username = row.username;
	} else {
		username = shaHash(row.username);
	}

	var trace = traceManager.get_trace(username);
	if (trace === undefined) {
		// TODO Get the url from a configuration file

		// dwe stands for Digital Work Environment
		trace = traceManager.init_trace(username, {
																			url: 'http://localhost:5000/trace/',
																			requestmode: 'POST',
																			syncmode: 'sync',
																			default_subject: 'dwe_trace',
																			format: 'json'
		});

		tr.trace('LogFound');
	}

	callback();
}

function transformLogs(err, rows, fields) {
	if (err) throw err;

	async.each(rows, function buildTraceFromRow(row, callback) {
		extractLog(row, callback);
	}, function allRowsHandled(err) {
		if (err) {
			console.log('An error has occured.');
			throw err;
		} else {
			console.log('Done.');
		}
	});
}

requirejs.config({
	nodeRequire: require
});

function extractLogs() {
	var connection = createMySqlConnection();
	connection.connect();
	connection.query('SELECT log.id, log.time, log.userid, log.ip, ' +
							            'log.course, log.module, log.cmid, log.action, ' +
							            'log.url, log.info, course.category, ' +
							            'user.username ' +
					         'FROM (SELECT * FROM mdl_log log ' +
					 	       'WHERE log.action = \'view\' ' +
					 	       'AND log.module = \'resource\' ' +
					 	       'ORDER BY log.id asc LIMIT 1000) AS log ' +
					         'JOIN mdl_course course ON course.id = log.course ' +
					         'JOIN mdl_user user ON user.id = log.userid',
					         transformLogs);
	connection.end();	
}

requirejs(['jquery', 'tracemanager'], function(requiredjQuery, 
	                                             requiredTraceManager) {
	console.log('Chargement effectué');

	console.log(requiredTraceManager);

	jQuery = requiredjQuery;
	traceManager = requiredTraceManager;

	console.log(jQuery);
	console.log(requiredTraceManager);

	extractLogs();
});