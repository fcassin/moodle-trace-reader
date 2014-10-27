var pg = require('pg');
var connString = 'postgres://postgres@localhost/moodle-trace-reader';

pg.connect(connString, function(err, client, done) {
	if (err) {
		return console.error('error fetching client from pool', err);
	}
	client.query('INSERT INTO test VALUES(nextval(\'test_id_seq\'), $1::varchar)', ['test'], function(err, result) {
		done();

		if (err) {
			return console.error('error running query', err);
		}

		console.log('insert successful');
	})
})