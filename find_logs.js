var aggregate = {};

var async = require('async');
var mongo = require('./ktbs_mongo.js');

aggregate.findLogs = function(args, callback) {
	console.log('Finding logs with args : ' + JSON.stringify(args, null, 2));

	connection = mongo.createMongoConnection();
	obselModel = mongo.getObselModel(connection);

	obselModel.count(args, function(err, results) {
		if (err) return callback(err);

		if (results > 100000) {
			mongo.disconnectMongo(connection);
			throw 'More than 100,000 results found. Please be more specific in your request';
		} else {
			obselModel.find(args, function(err, results) {
				if (err) return callback(err);

				mongo.disconnectMongo(connection);
				callback(null, results);
			});
		}
	});
}

aggregate.findByFirstLevelModule = function(moduleId, callback) {
	var args = {};
  args['course.category1.moodleId'] = moduleId;

	aggregate.findLogs(args, function(err, results) {
		if (err) return callback(err);

		callback(err, results);
	})
}

// TODO Filter by day and iterate over the days
aggregate.findBySecondLevelModule = function(moduleId, callback) {
	var args = {};
  args['course.category2.moodleId'] = moduleId;

  var novemberFirst = new Date(2014,10,1);
  var novemberSecond = new Date(2014,10,2);

  args['begin'] = { $gt : novemberFirst.getTime() };
  args['end'] = { $lt : novemberSecond.getTime() };

	aggregate.findLogs(args, function(err, results) {
		if (err) return callback(err);

		callback(err, results);
	})
}

module.exports = aggregate;