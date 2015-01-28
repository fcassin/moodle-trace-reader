var finder = require('./find_logs');

var async = require('async');
var fileSystem = require('fs');
var crypto = require('crypto');

var encoding = 'UTF-8';

// Creates a sha hash from parameter toHash
function shaHash(toHash) {
	var	shaSum = crypto.createHash('sha1');
	shaSum.update(toHash);
	var digest = shaSum.digest('hex');
	return digest;
}

function buildDateArray(fromDate, toDate) {
	var array = [];
	var currentDate = fromDate;

	var index = 0;
	while (currentDate <= toDate) {
		array[index] = currentDate;
		index += 1;
		currentDate = new Date(currentDate.getFullYear(),
													 currentDate.getMonth(),
													 currentDate.getDate() + 1);
	}

	return array;
}

function exportToCSV() {
	var firstDay = new Date(2014,7,15);
	var lastDay = new Date(2014,11,31);
  //var decemberThirtyFirst = new Date(2014,11,1);

  var dateArray = buildDateArray(firstDay, lastDay);
  var currentYear = firstDay.getFullYear();
  var currentMonth = firstDay.getMonth();
  var writeStream = initCSVFile(currentYear, currentMonth);

  async.eachSeries(dateArray, function(date, callback) {
  	console.log(date);

  	var iteratingYear = date.getFullYear();
  	var iteratingMonth = date.getMonth();

  	if (iteratingMonth > currentMonth || iteratingYear > currentYear) {
  		currentYear = iteratingYear;
  		currentMonth = iteratingMonth;

  		writeStream = initCSVFile(currentYear, currentMonth);
  	}

  	var tomorrow = new Date(date.getFullYear(),
  													date.getMonth(),
  													date.getDate() + 1);

  	finder.findBySecondLevelModule(381, date, tomorrow, function(err, results) {
  		if (err) return callback(err);

  		if (results.length > 0) {
  			console.log('Writing results to csv file');

	  		writeResultsToCSV(writeStream, results, function(err, results) {
	  			console.log('Results written');
	  			callback();
	  		});
  		} else {
  			console.log('No results');
  			callback();
  		}
  	})
  }, function then(err) {
  	if (err) throw err;

		console.log('Done');
  })
}

exportToCSV();

function findPACESResults(callback) {
	var novemberFirst = new Date(2014,10,1);
  var novemberSecond = new Date(2014,10,2);

	finder.findBySecondLevelModule(381, novemberFirst, novemberSecond, function(err, results) {
		if (err) callback(err);

		console.log(results.length + ' results found.');

		callback(null, results);
	});
}	

/*function findResults(callback) {
	finder.findBySecondLevelModule(381, function(err, results) {
		if (err) callback(err);

		console.log(results.length + ' results found.');

		callback(null, results);
	});
}*/

function buildCSVHeader() {
	var buffer = '';

	buffer = buffer + 'moodleId;';
	buffer = buffer + 'type;';
	buffer = buffer + 'begin;';
	buffer = buffer + 'end;';
	buffer = buffer + 'subject;';
	buffer = buffer + 'student;';
	buffer = buffer + 'added;';
	buffer = buffer + 'course.moodleId;';
	buffer = buffer + 'course.name;';
	buffer = buffer + 'course.category1.moodleId;';
	buffer = buffer + 'course.category1.name;';
	buffer = buffer + 'course.category2.moodleId;';
	buffer = buffer + 'course.category2.name;';
	buffer = buffer + 'course.category3.moodleId;';
	buffer = buffer + 'course.category3.name;';
	buffer = buffer + 'course.category4.moodleId;';
	buffer = buffer + 'course.category4.name;';
	buffer = buffer + 'resource.moodleId;';
	buffer = buffer + 'resource.deleted;';
	buffer = buffer + 'resource.author;';
	buffer = buffer + 'resource.fileName;';
	buffer = buffer + 'resource.displayName;';
	buffer = buffer + 'resource.mimeType;';
	buffer = buffer + 'resource.fileSize;';

	buffer = buffer + 'moodleId;';
	buffer = buffer + 'deleted;';
	buffer = buffer + 'intro;';
	buffer = buffer + 'name;';
	buffer = buffer + 'type;';
	buffer = buffer + 'discussionMoodleId;';
	buffer = buffer + 'discussionName;';
	buffer = buffer + 'postMoodleId;';
	buffer = buffer + 'postSubject;';
	buffer = buffer + 'postMessage';

	var buffer = buffer + '\n';
	return buffer;
}

function formatCategoryToCSV(result, category, buffer) {
	var moodleId = '';
	var name = '';
	var category = result.course[category];
	if (category !== undefined) {
		moodleId = category.moodleId;
		name = category.name;
	}

	buffer = buffer + moodleId;
	buffer = buffer + ';';

	buffer = buffer + name;
	buffer = buffer + ';';
	
	return buffer;
}

function formatForumToCSV(result, buffer) {
	var moodleId = '';
	var deleted = '';
	var intro = '';
	var name = '';
	var type = '';
	var discussionName = '';
	var discussionMoodleId = '';
	var postMoodleId = '';
	var postSubject = '';
	var postMessage = '';

	var forum = result.forum;

	if (forum != undefined && forum != null && forum != 'null') {
		deleted = forum.deleted;

		if (!deleted) {
			moodleId = forum.moodleId;
			intro = forum.intro;
			name = forum.name;
			type = forum.type;

			intro = intro.name;
			if (intro != undefined) {
				intro = intro.replace(/;/g, '.,');
			} else {
				intro = '';
			}

			var discussion = forum.discussion;

			if (discussion != undefined) {
				discussionMoodleId = discussion.moodleId;
				discussionName = discussion.name;
				discussionName = discussionName.replace(/;/g, '.,');

				var post = discussion.post;
				
				if (post != undefined) {

					postMoodleId = post.moodleId;
					postSubject = post.subject;
					postSubject = postSubject.replace(/;/g, '.,');
					postMessage = post.message;
					postMessage = postMessage.replace(/;/g, '.,');
					postMessage = postMessage.replace(/\r?\n/g, '');
				}
			}
		}
	}
	buffer = buffer + moodleId;
	buffer = buffer + ';';

	buffer = buffer + deleted;
	buffer = buffer + ';';

	buffer = buffer + intro;
	buffer = buffer + ';';

	buffer = buffer + name;
	buffer = buffer + ';';

	buffer = buffer + type;
	buffer = buffer + ';';

	buffer = buffer + discussionMoodleId;
	buffer = buffer + ';';

	buffer = buffer + discussionName;
	buffer = buffer + ';';

	buffer = buffer + postMoodleId;
	buffer = buffer + ';';

	buffer = buffer + postSubject;
	buffer = buffer + ';';

	buffer = buffer + postMessage;

	return buffer;
}

function formatResourceToCSV(result, buffer) {
	var moodleId = '';
	var deleted = '';
	var author = '';
	var fileName = '';
	var displayName = '';
	var mimeType = '';
	var fileSize = '';

	var resource = result.resource;

	if (resource != undefined && resource != null && resource != 'null') {
		deleted = resource.deleted;
		if (!deleted) {
			moodleId = resource.moodleId;
			deleted = resource.deleted;
			author = resource.author;
			fileName = resource.fileName;
			displayName = resource.displayName;
			mimeType = resource.mimeType;
			fileSize = resource.fileSize;
		}
	}
	buffer = buffer + moodleId;
	buffer = buffer + ';';

	buffer = buffer + deleted;
	buffer = buffer + ';';

	buffer = buffer + author;
	buffer = buffer + ';';

	buffer = buffer + fileName;
	buffer = buffer + ';';

	buffer = buffer + displayName;
	buffer = buffer + ';';

	buffer = buffer + mimeType;
	buffer = buffer + ';';

	buffer = buffer + fileSize;
	buffer = buffer + ';';

	return buffer;
}

function formatToCSV(result) {
	var buffer = '';
	
	buffer = buffer + result.attr.moodleId;
	buffer = buffer + ';';

	buffer = buffer + result['@type'];
	buffer = buffer + ';';
	
	buffer = buffer + result.begin.toString();
	buffer = buffer + ';';

	buffer = buffer + result.end.toString();
	buffer = buffer + ';';

	var subject = result.subject;
	if (result.subject != 'guest') {
		subject = shaHash(subject);
	}

	buffer = buffer + subject;
	buffer = buffer + ';';

	buffer = buffer + result.student;
	buffer = buffer + ';';

	buffer = buffer + result.added.toString();
	buffer = buffer + ';';

	buffer = buffer + result.course.moodleId;
	buffer = buffer + ';';

	buffer = buffer + result.course.name;
	buffer = buffer + ';';

	buffer = formatCategoryToCSV(result, 'category1', buffer);
	buffer = formatCategoryToCSV(result, 'category2', buffer);
	buffer = formatCategoryToCSV(result, 'category3', buffer);
	buffer = formatCategoryToCSV(result, 'category4', buffer);

	buffer = formatResourceToCSV(result, buffer);

	buffer = formatForumToCSV(result, buffer);

	buffer = buffer + '\n';
	return buffer;
}

function initCSVFile(year, month) {
	var writeStream = fileSystem.createWriteStream('./output-' + year + '-' + month + '.csv');
	writeStream.write(buildCSVHeader(), encoding);
	return writeStream;
}

function writeResultsToCSV(writeStream, results, callback) {
	var length = results.length;

	if (length > 0) {
		
		write();
		function write() {
			var ok = true;

			do {
				length -= 1;
				if (length === 0) {
					writeStream.write(formatToCSV(results[length]), encoding, callback);
				} else {
					ok = writeStream.write(formatToCSV(results[length]), encoding);
				}
			} while (length > 0 && ok);
			if (length > 0) {
				//console.log('Waiting for drain event');
				writeStream.once('drain', write);
			}
		}
	}
}

/*findPACESResults(function(err, results) {
	writeResultsToCSV(results, function() {
		console.log('Done');
	})
});*/
