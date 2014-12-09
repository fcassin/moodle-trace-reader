var aggregateLoginsByDate = require('./aggregate_login_events_by_date');
var aggregateUsersByDate = require('./aggregate_unique_users_by_date');
var aggregateCategoryByDate = require('./aggregate_logs_by_category_and_date');
var aggregateUserLogsByModuleAndDate = require('./aggregate_logs_by_user_module_and_date');
var orderCategoriesByLogs = require('./order_categories_by_logs');
var orderStudentsByCategory = require('./order_students_by_category');

var async = require('async');

async.series([
		function clearPreviousResults(callback) {
			async.parallel([
				function clearLogins(callback) {
					aggregateLoginsByDate.clearPreviousResults(callback);
				},
				function clearUsers(callback) {
					aggregateUsersByDate.clearPreviousResults(callback);
				},
				function clearCategories(callback) {
					aggregateCategoryByDate.clearPreviousResults(callback);
				}, function clearOrderedCategories(callback) {
					orderCategoriesByLogs.clearPreviousResults(callback);
				}
			], function allCleared(err) {
				if (err) callback(err);
				console.log('All cleared');
				callback();
			});
		},
		function aggregateResults(callback) {
			async.parallel([
				function aggregateLogins(callback) {
					aggregateLoginsByDate.groupByDateThenStore(callback);
				},
				function aggregateUsers(callback) {
					aggregateUsersByDate.groupLoginsByDateAndUserThenStore(callback);
				},
				function aggregateCategories(callback) {
					aggregateCategoryByDate.groupLogsByDateAndCategoryThenStore(callback);
				},
				function orderCategories(callback) {
					orderCategoriesByLogs.orderCategoriesByLogsThenStore(callback);
				}
			], function aggregationsDone(err) {
				if (err) callback(err);
				console.log('All aggregations were successful');
				callback();
			});			
		},
		function findResults(callback) {
			//aggregateUserLogsByModuleAndDate.findOrComputeUserLogs(
				//'119fc8dcc7d2c7bdfff06d1446b714941429d6d9',
			orderStudentsByCategory.findOrComputeStudentsByCategory(
				377, 
				function(err, results) {
				if (err) callback(err);

				console.log('Results : ' + JSON.stringify(results[0].results, null, 2));
				callback();
			});
		}
	], function handleErrors(err) {
		if (err) throw err;
		console.log('Done.');
	});