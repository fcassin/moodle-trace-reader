var async = require('async');
var crypto = require('crypto');
var mysql = require('mysql');
var uuid = require('node-uuid');

var ktbsMongo = require('./ktbs_mongo.js');

var category_buffer = {};

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

function isStudentUsername(username) {
	if (username.length === 8) {
		if (username.indexOf('e') === 0) {
			if (username.charAt(7).match(/[a-z]/i)) {
				return true;
			}
		}
	}
	return false;
}

function dateInSecToDateInMillis(date) {
	var dateInMillis = date + '000';
	return dateInMillis;
}

// A quoi correspond le contenu de la table file récupéré avec la premiere requete ?
// On dirait une historisation de plusieurs versions d'un meme fichier
// Voir la ligne de log d'id 92935580
function setResourceInformation(cmid, info, obsel, callback) {
	var resource = { deleted : false };

	var query1 = 'SELECT f.id, f.filepath, f.filename, f.filesize, ' + 
										 'f.mimetype, f.author ' +
							'FROM mdl_files f, mdl_context c, mdl_course_modules cm ' +
							'WHERE f.contextid = c.id ' +
							'AND f.filename <> \'.\' ' +
							'AND c.instanceid = cm.id ' +
							'AND c.contextlevel = 70 ' +
							'AND cm.id = ' + cmid + ' ' +
							'ORDER BY f.id DESC';

	var query2 = 'SELECT * FROM mdl_resource WHERE id = ' + info;

	var connection = createMySqlConnection();
	connection.connect();
	connection.query(
		query1,
	  function(err, rows1, fields) {
	 		if (err) return callback(err);

			if (rows1.length > 0) {
				connection.query(
					query2,
					function(err, rows2, fields) {
						var fileInfoRow = rows1[0];
						var resourceNameRow = rows2[0];

						resource.displayName = resourceNameRow.name;
						resource.fileName = fileInfoRow.filename;
						resource.mimeType = fileInfoRow.mimetype;
						resource.author = fileInfoRow.author;
						resource.fileSize = fileInfoRow.filesize;
						resource.moodleId = fileInfoRow.id;

						obsel.resource = resource;

						connection.end();
						callback();
					}
				);
			} else {
				resource.deleted = true;
				obsel.resource = resource;
				connection.end();
				callback();
			}
	  }
	);
}

function setCategoryInformation(category, depth, obsel, callback) {
	var buffer_key = depth + '_' + category;
	var buffer_value = category_buffer[buffer_key];

	if (buffer_value) {
		for (var property in buffer_value) {
			if (buffer_value.hasOwnProperty(property)) {
				obsel.course[property] = buffer_value[property];
			}
		}
		callback();
	} else {
		if (category > 0) {
			var query = 'SELECT ';
		
			for (var i = 1; i <= depth; i++) {
				query = query + 'cc' + i + '.id ';
				query = query + 'AS cc' + i + '_id, ';
				query = query + 'cc' + i + '.name ';
				query = query + 'AS cc' + i + '_name';
				if (i != depth) query = query + ',';
				query = query + ' ';
			}

			query = query + 'FROM ';

			for (var i = 1; i <= depth; i++) {
				query = query + 'mdl_course_categories cc' + i;
				if (i != depth) query = query + ',';
				query = query + ' ';
			}

			query = query + 'WHERE ';

			for (var i = 1; i <= depth; i++) {
				if (i != depth) {
					query = query + 'cc' + i + '.id = cc' + (i+1) + '.parent AND ';
				} else {
					query = query + 'cc' + i + '.id = ' + category; 
				}
			}

			var connection = createMySqlConnection();
			connection.connect();
			connection.query(
				query,
        function(err, rows, fields) {
        	if (err) return callback(err);

        	connection.end();

          if (rows.length == 1) {
          	var row = rows[0];
          	var buffer_value = {};

          	for (var i = 1; i <= depth; i++) {
          		var categoryInfo = {};
          		categoryInfo.name = row['cc' + i + '_name'];
          		categoryInfo.moodleId = row['cc' + i + '_id'];

          		buffer_value['category' + i] = categoryInfo;
          	}

          	category_buffer[buffer_key] = buffer_value;

          	setCategoryInformation(category, depth, obsel, callback);
          }
        }); 
		} else {
			// No category for this course
			callback(); 
		}	
	}
}

function extractLog(row, connection, callback) {
	var username;
	var isStudent = isStudentUsername(row.username);

	if (row.username === 'guest') {
		username = row.username;
	} else {
		// We choose not to hide student ids immediatly for now
		// However, it MUST be done for the final extraction
		//username = shaHash(row.username);
		username = row.username;
	}

	var dateInMillis = dateInSecToDateInMillis(row.time);

	var obselModel = ktbsMongo.getObselModel(connection);

	var obsel = new obselModel({
		/*'_serverid' : generate_uuid(),*/
		'@type' : row.module + '_' + row.action,
		begin : dateInMillis,
		end : dateInMillis,
		subject : username,
		student : isStudent,
		attr : { moodleId : row.id },
		course : { moodleId : row.course_id, name : row.course_fullname }
	});

	async.series([
		// Get the category information from memory
		// If unavailable, load it from database and store it in memory
		function categoryInformation(callback) {
			setCategoryInformation(row.category, row.depth, obsel, callback);		
		}, function resourceInformation(callback) {
			if (row.module === 'resource') {
				setResourceInformation(row.cmid, row.info, obsel, callback);
			} else {
				callback();
			}
		}, function saveObsel(callback) {
			obsel.save(function(err) {
				if (err) return callback(err);
				callback();
			});
		}
	], function errorHandler(err) {
		if (err) return callback(err);
		callback();
	});
}

function transformLogs(err, rows, fields, callback) {
	if (err) throw err;

	var connection = ktbsMongo.createMongoConnection();

	async.eachSeries(rows, function buildTraceFromRow(row, callback) {
		extractLog(row, connection, callback);
	}, function allRowsHandled(err) {
		if (err) {
			console.log('An error has occured.');
			throw err;
		} else {
			ktbsMongo.disconnectMongo(connection);
			callback(null, rows.length);
		}
	});
}

function findMostRecentTraceId(mostRecent, callback) {
	var connection = ktbsMongo.createMongoConnection();

	var obselModel = ktbsMongo.getObselModel(connection);

	obselModel.find()
						.sort({ 'attr.moodleId' : -1})
						.limit(1)
						.exec(function (err, results) {
		if (err) return callback(err);

		if (results && results.length > 0) {
			mostRecent.id = results[0].attr.moodleId;
		}
		ktbsMongo.disconnectMongo(connection);
		callback();
	});
}

function extractLogBatch(mostRecent, callback) {
	var connection = createMySqlConnection();
	connection.connect();
	connection.query('SELECT log.id, log.time, log.userid, log.ip, ' +
							            'log.course, log.module, log.cmid, log.action, ' +
							            'log.url, log.info, course.category, ' +
							            'course.id AS course_id, ' + 
							            'course.fullname AS course_fullname, ' + 
							            'cc1.id AS category_id, ' +
							            'cc1.name AS category_name, ' +
							            'cc1.depth AS depth, ' + 
							            'user.username ' +
					         'FROM (SELECT * FROM mdl_log log ' +
					 	       //'WHERE log.action = \'login\' ' +
					 	       //'AND log.module = \'user\' ' +
					 	       'WHERE log.id > ' + mostRecent.id + ' ' + 
					 	       'ORDER BY log.id asc LIMIT 10000) AS log ' +
					         'JOIN mdl_course course ON course.id = log.course ' +
					         'JOIN mdl_user user ON user.id = log.userid ' +
					         'LEFT JOIN mdl_course_categories AS cc1 ON course.category = cc1.id',
					         function(err, rows, fields) {
					           transformLogs(err, rows, fields, function(err, results) {
					           	 if (err) return callback(err);

					           	 connection.end();
					           	 return callback(null, results);
					           }); 
					         });
}

function extractLogs() {
	var mostRecent = { id : 0 };
	var insertedRows = 0;

	async.series([
		function mostRecentTrace(callback) {
			findMostRecentTraceId(mostRecent, callback);
		},
		function extractOneBatch(callback) {
			extractLogBatch(mostRecent, function(err, results) {
				if (err) return callback(err);

				insertedRows = results;
				callback();
			});
		}
	], function errorHandler(err) {
		if (err) {
			console.error('An error has occured.');
			throw err;
		}

		console.log(insertedRows + ' rows inserted.');

		if (insertedRows > 0) {
			//extractLogs();
		}
	});	
}

extractLogs();