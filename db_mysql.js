var node_mysql = require('node-mysql');
var mysql = require('mysql');
var pg = require('pg');

var async = require('async');
var crypto = require('crypto');

var postgresConnectionString = 'postgres://postgres@localhost/moodle-trace-reader';

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
	console.log('Transforming user ' + userId);

	var connection = createMySqlConnection();
	connection.connect();
	connection.query('SELECT id, username FROM mdl_user WHERE id = ' + userId,
	                 function loadActor(err, rows, fields) {
		
		if (err) return callback(err);

		if (rows.length == 0) {
			console.log('No user found for the following row');
			console.log(row);
			id = -1;
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
	console.log('Checking user ' + userId);

	// Checks if the user is already in the destination database
	var client = new pg.Client(postgresConnectionString);
	client.connect(function(err) {
		if (err) return callback(err);

		client.query('SELECT id, original_id FROM users WHERE original_id = $1', 
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

function transformResource(logRow) {
	var resourceId = logRow.info;
	if (manipulatedResources.indexOf(resourceId) == -1) {
		manipulatedResources.push(resourceId);

		var connection = createMySqlConnection();
		connection.connect();
		connection.query('SELECT resource.id, resource.name, resource.intro, files.mimetype, files.filename, files.filesize ' + 
						 'FROM mdl_resource resource, ' +
						      'mdl_course_modules course_modules, ' +
						      'mdl_context context, ' +
						      'mdl_files files ' +
						 'WHERE resource.id = ' + logRow.info + ' ' +
						 'AND course_modules.id = ' + logRow.cmid + ' ' +
						 'AND context.instanceid = course_modules.id AND contextlevel = 70 ' +
						 'AND files.contextid = context.id AND filename <> \'.\'', function loadResource(err, rows, fields) {
			
			if (err) throw err;

			if (rows.length == 0) {
				//console.log('No resource found for the following row');
				//console.log(logRow);
			} else {
				var row = rows[0];
				//console.log(row);
				var resourceId = row.id;

				pg.connect(postgresConnectionString, function(err, client, done) {
					if (err) throw err;

					client.query('INSERT INTO resources VALUES(nextval(\'resources_id_seq\'), $1, $2, $3, $4, $5)', [resourceId, row.intro, row.name, row.mimetype, row.filename], function(err, result) {
						done();
						if (err) throw err;

						//console.log('Création d\'une nouvelle resource');
						manipulatedResources.pop(resourceId);
						checkLog(logRow);
					});
				});
			}
		});

		connection.end(function(err) {
			if (err) throw err;
		});
	} else {
		//console.log('Resource existante');
		checkLog(logRow);
	}
}

function checkResource(row) {
	var resourceId = row.info;
	if (manipulatedResources.indexOf(resourceId) == -1) {
		// Verifie si la ressource est présente ou non dans la base de traitement Postgres
		pg.connect(postgresConnectionString, function(err, client, done) {
			if (err) throw err;

			client.query('SELECT original_id FROM resources WHERE original_id = $1', [ resourceId ], function continueOrInsertResource(err, result) {
				// Call done to release the client back to the pool
				done();
				if (err) throw err;

				if (result.rows.length == 0) {
					transformResource(row);
				} else {
					//console.log('Ressource existante');
					checkLog(row);
				}
			});
		});
	} else {
		//console.log('Ressource existante');
		checkLog(row);
	}
}

function transformCourse(logRow) {
	var courseModuleId = logRow.cmid;
	if (manipulatedModules.indexOf(courseModuleId) == -1) {
		manipulatedModules.push(courseModuleId);
		//console.log(logRow);

		var connection = createMySqlConnection();
		connection.connect();
		connection.query('SELECT * ' + 
						 'FROM mdl_course_modules course_modules ' +
						 'WHERE course_modules.id = ' + logRow.cmid + ' '
						 , function loadResource(err, rows, fields) {
			
			if (err) throw err;

			if (rows.length == 0) {
				//console.log('No resource found for the following row');
				//console.log(logRow);
			} else {
				var row = rows[0];
				//console.log(row);
				/*var courseModuleId = row.id;

				pg.connect(postgresConnectionString, function(err, client, done) {
					if (err) throw err;

					client.query('INSERT INTO resources VALUES(nextval(\'resources_id_seq\'), $1, $2, $3, $4, $5)', [resourceId, row.intro, row.name, row.mimetype, row.filename], function(err, result) {
						done();
						if (err) throw err;

						//console.log('Création d\'une nouvelle resource');
						manipulatedResources.pop(courseModuleId);
					});
				});*/
			}
		});

		connection.end(function(err) {
			if (err) throw err;
		});
		checkLog(logRow);
	} else {
		//console.log('Resource existante');
		checkLog(logRow);
	}
}

function checkCourse(row) {
	var courseId = row.course;
	//console.log(courseId);
	if (manipulatedCourses.indexOf(courseId) == -1) {
		pg.connect(postgresConnectionString, function(err, client, done) {
			if (err) throw err;

			client.query('SELECT original_id FROM course WHERE original_id = $1', 
						 [ courseId ], 
						 function continueOrInsertCourse(err, result) {
				// Call done to release the client back to the pool
				done();
				if (err) throw err;

				if (result.rows.length == 0) {
					transformCourse(row);
				} else {
					//console.log('Ressource existante');
					//checkLog(row);
				}
			});
		});
	} else {
		//console.log('Ressource existante');
		checkLog(row);
	}
}

function checkLog(row) {
	//console.log('Checking log');

	// Lancer tous les traitements en parallele ici ???
	// Comment catcher un event signifiant que toutes les opérations ont bien eu lieu ?
	// Necessité de catcher cet event pour avoir acces aux nouveaux identifiants
	// Les identifiants sont obligatiores pour créer les clefs etrangeres lors de l'insertion
	// du log en base
}

function transformLogs(err, rows, fields) {
	if (err) throw err;

	// For every log trace found
	async.eachSeries(rows, function(row, callback) {
		// First, handle all of its dimensions in a parallel way
		// Then insert the transformed trace into the database
		var user = { id : -1 };
		async.parallel([
			function(callback) {
				checkUser(row, user, callback);
			}/*,
			function(callback) {
				checkResource(row, callback);
			}*/
		], function(err) {
			// This function gets called after all of the parallels functions
			// have properly terminated
			if (err) return callback(err);

			console.log(user.id);
			callback();
		});
	}, function(err) {
		if (err) {
			console.log('An error has occured.');
			console.log(err);
		} else {
			console.log('Done.');
		}
	});

	/*for (var count = 0; count < rows.length; count++) {
		var row = rows[count];
		
	}*/
}

// TODO Ouvrir une connection sur la base postgres et vérifier le dernier log importé
// TODO Utiliser la librairie async pour controler le flow
function extractLogs() {
	var connection = createMySqlConnection();
	connection.connect();
	connection.query('SELECT * FROM mdl_log ' +
					 'WHERE action = \'view\' ' + 
					 'AND module = \'resource\' ' +
					 'ORDER BY ID asc LIMIT 5', transformLogs);
	connection.end();	
}

// TODO Itérer sur tous les logs
// Utilisation des vues sur les ressources uniquement pour l'instant pour construire le premier indicateur
extractLogs();

/*connection.query('SELECT count(*) as cnt from mdl_log', function(err, rows, fields) {
	if (err) throw err;

	console.log('Number of rows in mdl_log :', rows[0].cnt);
});*/