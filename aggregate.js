var aggregateLoginsByDate = require('./aggregate_login_events_by_date');
var aggregateUsersByDate = require('./aggregate_unique_users_by_date');
var async = require('async');

async.series([
		function clearPreviousResults(callback) {
			async.series([
				function clearLogins(callback) {
					aggregateLoginsByDate.clearPreviousResults(callback);
				},
				function clearUsers(callback) {
					aggregateUsersByDate.clearPreviousResults(callback);
				}
			], function allCleared(err) {
				if (err) callback(err);
				console.log('All cleared');
				callback();
			});
		},
		function aggregateResults(callback) {
			async.series([
				function aggregateLogins(callback) {
					aggregateLoginsByDate.groupByDateThenStore(callback);
				},
				function aggregateUsers(callback) {
					aggregateUsersByDate.groupLoginsByDateAndUserThenStore(callback);
				}
			], function aggregationsDone(err) {
				if (err) callback(err);
				console.log('All aggregations were successful');
				callback();
			});			
		},
		function findResults(callback) {
			aggregateLoginsByDate.findHourlyResults(function(err, results) {
				if (err) callback(err);

				console.log('Results : ' + results);
				callback();
			});
		}
	], function handleErrors(err) {
		if (err) throw err;
		console.log('Done.');
	});