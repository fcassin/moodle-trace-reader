var node_mysql = require('node-mysql');
var mysql = require('mysql');
var pg = require('pg');
var crypto = require('crypto');

var postgresConnectionString = 'postgres://postgres@localhost/moodle-trace-reader';

var manipulatedActors = [];

// Ouverture de la connection mySQL sur la base moodle
function create_mysql_connection() {
	var connection = mysql.createConnection({
		host : 'localhost',
		database : 'madoc',
		user : 'root',
		password : 'moodle'
	});

	return connection;
}

function load_resource(err, rows, fields) {
	if (err) throw err;

	console.log(rows[0]);
}

function shaHash(toHash) {
	var shaSum = crypto.createHash('sha1');
	shaSum.update(toHash);
	var digest = shaSum.digest('hex');
	return digest;
}

function load_actor(err, rows, fields) {
	if (err) throw err;

	var actor_id = rows[0].id;
	if (manipulatedActors.indexOf(actor_id) == -1) {
		manipulatedActors.push(actor_id);

		var hash = shaHash(rows[0].username);
		pg.connect(postgresConnectionString, function(err, client, done) {
			if (err) throw err;

			client.query('INSERT INTO users VALUES(nextval(\'users_id_seq\'), $1, $2)', [actor_id, hash], function(err, result) {
				done();
				if (err) throw err;

				console.log('Création d\' un nouvel utilisateur');
				manipulatedActors.pop(actor_id);
			});
		});
	} else {
		console.log('Utilisateur existant');
	}
}

function transform_actor(actorId) {
	var connection = create_mysql_connection();
	connection.connect();
	connection.query('SELECT id, username FROM mdl_user WHERE id = ' + actorId, load_actor);
	connection.end();
}

function check_actor(actorId) {
	// Verifie si l'acteur est présent ou non dans la base de traitement Postgres
	pg.connect(postgresConnectionString, function(err, client, done) {
		if (err) throw err;

		client.query('SELECT moodle_id FROM users WHERE moodle_id = $1', [ actorId ], function(err, result) {
			// Call done to release the client back to the pool
			done();
			if (err) throw err;

			if (result.rows.length == 0) {
				transform_actor(actorId);
			} else {
				console.log('Utilisateur existant');
			}
		});
	});
}

// TODO Ouvrir une connection sur la base postgres et vérifier le dernier log importé
function transform_resource(row) {
	//console.log(row);
	var connection = create_mysql_connection();
	connection.connect();
	connection.query('SELECT * FROM mdl_resource resource, ' +
					  			   'mdl_course_modules course_modules, ' +
					  			   'mdl_context context, ' +
					  			   'mdl_files files ' +
					  		  'WHERE resource.id = ' + row.info + ' ' +
					  		  'AND course_modules.id = ' + row.cmid + ' ' +
					  		  'AND context.instanceid = course_modules.id AND contextlevel = 70 ' +
					  		  'AND files.contextid = context.id AND filename <> \'.\'', load_resource);
	connection.end();
}

function transform_logs(err, rows, fields) {
	if (err) throw err;

	for (var count = 0; count < rows.length; count++) {
		var row = rows[count];
		if (row.module = 'resource' && row.action == 'view') {
			check_actor(row.userid);
			//transform_resource(row);
		}
	}
}

function extract_logs() {
	var connection = create_mysql_connection();
	connection.connect();
	connection.query('SELECT * FROM mdl_log WHERE action = \'view\' AND module = \'resource\' ORDER BY ID asc LIMIT 50', transform_logs);
	connection.end();	
}

// TODO Itérer sur tous les logs
// Utilisation des vues sur les ressources uniquement pour l'instant pour construire le premier indicateur
extract_logs();

/*connection.query('SELECT count(*) as cnt from mdl_log', function(err, rows, fields) {
	if (err) throw err;

	console.log('Number of rows in mdl_log :', rows[0].cnt);
});*/