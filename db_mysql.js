var node_mysql = require('node-mysql');
var mysql = require('mysql');
var pg = require('pg');

var async = require('async');
var crypto = require('crypto');

var postgresConnectionString = 'postgres://postgres@localhost/moodle-trace-reader';

var TRACE_TYPE_OBSEL = 1;
var TRACE_TYPE_AGGREG = 2;

// Ouverture de la connection mySQL sur la base moodle
function createMySqlConnection() {
	var connection = mysql.createConnection({
		host : 'localhost',
		database : 'madoc',
		user : 'root',
		password : 'moodle'
	});

	return connection;
}

function shaHash(toHash) {
	var shaSum = crypto.createHash('sha1');
	shaSum.update(toHash);
	var digest = shaSum.digest('hex');
	return digest;
}

function transformUser(row, user, callback) {
	var userId = row.userid;

	var connection = createMySqlConnection();
	connection.connect();
	connection.query('SELECT id, username FROM mdl_user WHERE id = ' + userId,
	                 function loadActor(err, rows, fields) {
		
		if (err) return callback(err);

		if (rows.length == 0) {
			console.error('No user found for the following row');
			console.error(row);
			user.id = -1;
			callback();
		} else {
			var userId = rows[0].id;
			var hash = shaHash(rows[0].username);

			var client = new pg.Client(postgresConnectionString);
			client.connect(function(err) {
				if (err) return callback(err);

				client.query('INSERT INTO users ' +
							 'VALUES(nextval(\'users_id_seq\'), $1, $2)', 
							 [userId, hash], 
							 function(err, result) {

					if (err) return callback(err);

					client.end();

					checkUser(row, user, callback);
				});
			});
		}	
	});

	connection.end(function(err) {
		if (err) return callback(err);
	});
}

function checkUser(row, user, callback) {
	var userId = row.userid;

	// Checks if the user is already in the destination database
	var client = new pg.Client(postgresConnectionString);
	client.connect(function(err) {
		if (err) return callback(err);

		client.query('SELECT id, original_id FROM users ' +
					 'WHERE original_id = $1', 
					 [ userId ], 
					 function continueOrInsertActor(err, result) {

			if (err) return callback(err);

			client.end();

			if (result.rows.length == 0) {
				transformUser(row, user, callback);
			} else {
				user.id = result.rows[0].id;
				callback();
			}
		});
	});
}

function transformResource(row, resource, callback) {
	var resourceId = row.info;

	var connection = createMySqlConnection();
	connection.connect();
	connection.query('SELECT resource.id, resource.name, resource.intro, ' +
							'files.mimetype, files.filename, files.filesize ' + 
					 'FROM mdl_resource resource, ' +
					      'mdl_course_modules course_modules, ' +
					      'mdl_context context, ' +
					      'mdl_files files ' +
					 'WHERE resource.id = ' + row.info + ' ' +
					 'AND course_modules.id = ' + row.cmid + ' ' +
					 'AND context.instanceid = course_modules.id ' +
					 'AND contextlevel = 70 ' +
					 'AND files.contextid = context.id ' +
					 'AND filename <> \'.\'', 
					 function loadResource(err, rows, fields) {
		
		if (err) return callback(err);

		if (rows.length == 0) {
			// Write to the console or fail silently ?
			console.error('No resource found for the following row');
			console.error(row);
			resource.id = -1;
			callback();
		} else {
			var resourceId = rows[0].id;

			var client = new pg.Client(postgresConnectionString);
			client.connect(function(err) {
				if (err) return callback(err);

				client.query('INSERT INTO resources ' +
							 'VALUES(nextval(\'resources_id_seq\'), ' +
							 	    '$1, $2, $3, $4, $5)', 
							 [resourceId, rows[0].intro, rows[0].name, 
							 	rows[0].mimetype, rows[0].filename], 
							 function(err, result) {

					if (err) return callback(err);

					client.end();
					
					checkResource(row, resource, callback);
				});
			});
		}
	});

	connection.end(function(err) {
		if (err) throw err;
	});
}

function checkResource(row, resource, callback) {
	var resourceId = row.info;

	// Checks if the resource is already in the destination database
	var client = new pg.Client(postgresConnectionString);
	client.connect(function(err) {
		if (err) return callback(err);

		client.query('SELECT id, original_id FROM resources ' + 
			         'WHERE original_id = $1', 
			         [ resourceId ], 
			         function continueOrInsertResource(err, result) {
			
			if (err) return callback(err);

			client.end();

			if (result.rows.length == 0) {
				transformResource(row, resource, callback);
			} else {
				resource.id = result.rows[0].id;
				callback();
			}
		});
	});
}

function transformCourseCategory(row, courseCategory, callback) {
	var courseCategoryId = row.category;

	var connection = createMySqlConnection();
	connection.connect();
	connection.query('SELECT categories.id, categories.name, ' + 
							'categories.description, categories.parent ' + 
					 'FROM mdl_course_categories categories ' +
					 'WHERE categories.id = ' + courseCategoryId + ' '
					 , function loadResource(err, rows, fields) {
		
		if (err) return callback(err);

		if (rows.length == 0) {
			// Write to the console or fail silently ?
			console.error('No course category found for the following row');
			console.error(row);
			courseCategory.id = -1;
			callback();
		} else {
			var parentCourseCategory = { id : null };

			async.series([
				function checkCourseCategoryHierarchy(callback) {
					if (rows[0].parent) {
						checkCourseCategory({ category : rows[0].parent},
											parentCourseCategory,
											callback);
					} else {
						// Nothing to do in this case
						callback();
					}
				}
			], function parentCategoryHandled(err) {
				if (err) return callback(err);
				
				var client = new pg.Client(postgresConnectionString);
				client.connect(function(err) {
					if (err) return callback(err);

					client.query('INSERT INTO course_categories ' +
								 'VALUES(nextval(\'course_categories_id_seq\'), ' +
								 		'$1, $2, $3, $4)',
								 [courseCategoryId, rows[0].name, 
								 	rows[0].description, parentCourseCategory.id],
								  function(err, result) {

							if (err) return callback(err);

							client.end();

							checkCourseCategory(row, courseCategory, callback);
					  	}
					);
				});
			});
		}
	});

	connection.end(function(err) {
		if (err) throw err;
	});
}

function checkCourseCategory(row, courseCategory, callback) {
	var courseCategoryId = row.category;
	
	// Checks if the resource is already in the destination database
	var client = new pg.Client(postgresConnectionString);
	client.connect(function(err) {
		if (err) return callback(err);

		client.query('SELECT id, original_id FROM course_categories ' +
					 'WHERE original_id = $1', 
					 [ courseCategoryId ], 
					 function continueOrInsertCourseCategory(err, result) {

			if (err) return callback(err);

			client.end();

			if (result.rows.length == 0) {
				transformCourseCategory(row, courseCategory, callback);
			} else {
				courseCategory.id = result.rows[0].id;
				callback();
			}
		});
	});
}

function transformCourse(row, course, courseCategory, callback) {
	var courseId = row.course;

	var connection = createMySqlConnection();
	connection.connect();
	connection.query('SELECT course.id, course.fullname, course.shortname, ' +
							'course.summary ' + 
					 'FROM mdl_course course ' +
					 'WHERE course.id = ' + courseId, 
					 function loadCourse(err, rows, fields) {
		
		if (err) return callback(err);

		if (rows.length == 0) {
			// Write to the console or fail silently ?
			console.error('No course found for the following row');
			console.error(row);
			course.id = -1;
			callback();
		} else {
			var client = new pg.Client(postgresConnectionString);
			client.connect(function(err) {
				if (err) return callback(err);

				client.query('INSERT INTO courses ' +
							 'VALUES(nextval(\'courses_id_seq\'), ' +
							 	    '$1, $2, $3, $4, $5)', 
							 [rows[0].id, rows[0].shortname, rows[0].fullname, 
							 	rows[0].summary, courseCategory.id], 
							 function(err, result) {

					if (err) return callback(err);

					client.end();
					
					checkCourse(row, course, courseCategory, callback);
				});
			});
		}
	});

	connection.end(function(err) {
		if (err) throw err;
	});
}

function checkCourse(row, course, courseCategory, callback) {
	var courseId = row.course;

	// Checks if the resource is already in the destination database
	var client = new pg.Client(postgresConnectionString);
	client.connect(function(err) {
		if (err) return callback(err);

		client.query('SELECT id, original_id FROM courses ' + 
			         'WHERE original_id = $1', 
			         [ courseId ], 
			         function continueOrInsertCourse(err, result) {
			
			if (err) return callback(err);

			client.end();

			if (result.rows.length == 0) {
				transformCourse(row, course, courseCategory, callback);
			} else {
				course.id = result.rows[0].id;
				callback();
			}
		});
	});
}

function transformLog(row, user, resource, course, callback) {
	if (user.id != -1 && resource.id != -1 && course.id != -1) {
		var client = new pg.Client(postgresConnectionString);
		client.connect(function(err) {
			if (err) return callback(err);

			client.query('INSERT INTO traces ' +
						 'VALUES(nextval(\'traces_id_seq\'), ' +
						 	    '$1, $2, $3, $4, $5, $6, ' +
						 	'(SELECT TIMESTAMP WITH TIME ZONE ' +
						 		'\'epoch\' + $7 * INTERVAL \'1 second\'), ' +
								'$8)', 
						 [row.id, TRACE_TYPE_OBSEL, course.id, user.id, 
						  row.module, row.action, row.time, resource.id], 
						 function(err, result) {

				if (err) return callback(err);

				client.end();
				
				checkLog(row, user, resource, course, callback);
			});
		});
	} else {
		console.error('Missing context, not saving log information');
		callback();
	}
}

function checkLog(row, user, resource, course, callback) {
	var logId = row.id;

	// Checks if the resource is already in the destination database
	var client = new pg.Client(postgresConnectionString);
	client.connect(function(err) {
		if (err) return callback(err);

		client.query('SELECT id, original_id FROM traces ' + 
			         'WHERE original_id = $1', 
			         [ logId ], 
			         function continueOrInsertLog(err, result) {
			
			if (err) return callback(err);

			client.end();

			if (result.rows.length == 0) {
				transformLog(row, user, resource, course, callback);
			} else {
				callback();
			}
		});
	});
}

function transformLogs(err, rows, fields) {
	if (err) throw err;

	// For every log trace found
	async.eachSeries(rows, function(row, callback) {
		// First, handle all of its dimensions in a parallel way
		// Then insert the transformed trace into the database
		var user = { id : null };
		var resource = { id : null };
		var courseCategory = { id : null };
		var course = { id : null };
		
		async.parallel([
			function buildUserDimension(callback) {
				checkUser(row, user, callback);
			},
			function buildResourceDimension(callback) {
				checkResource(row, resource, callback);
			},
			function buildCourseDimension(callback) {
				async.series([
					function buildCourseCategoriesDimension(callback) {
						checkCourseCategory(row, courseCategory, callback);
					},
					function buildCourseDimension(callback) {
						checkCourse(row, course, courseCategory, callback);
					}
				], function courseDimensionsBuilt(err) {
					if (err) return callback(err);

					callback();
				});
			}
		], function allDimensionsBuiltForLogRow(err) {
			// This function gets called after all of the parallels functions
			// have properly terminated
			if (err) return callback(err);

			checkLog(row, user, resource, course, callback);
		});
	}, function allLogRowsImported(err) {
		if (err) {
			console.log('An error has occured.');
			throw err;
		} else {
			console.log('Done.');
		}
	});
}

// TODO Ouvrir une connection sur la base postgres et vérifier le dernier log importé
// DOING Utiliser la librairie async pour controler le flow
function extractLogs() {
	var connection = createMySqlConnection();
	connection.connect();
	connection.query('SELECT log.id, log.time, log.userid, log.ip, ' +
							'log.course, log.module, log.cmid, log.action, ' +
							'log.url, log.info, course.category ' +
					 'FROM (SELECT * FROM mdl_log log ' +
					 	   'WHERE log.action = \'view\' ' +
					 	   'AND log.module = \'resource\' ' +
					 	   'ORDER BY log.id asc LIMIT 10000) AS log ' +
					 'JOIN mdl_course course on course.id = log.course',
					 transformLogs);
	connection.end();	
}

// TODO Itérer sur tous les logs Utilisation des vues sur les ressources
// uniquement pour l'instant pour construire le premier indicateur
extractLogs();