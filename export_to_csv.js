var finder = require('./find_logs');

var async = require('async');
var fileSystem = require('fs');

function findPACESResults(callback) {
	finder.findBySecondLevelModule(381, function(err, results) {
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
	buffer = buffer + 'resource.fileSize';

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
		moodleId = resource.moodleId;
		deleted = resource.deleted;
		if (!deleted) {
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

	buffer = buffer + result.subject;
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

	buffer = buffer + '\n';
	return buffer;
}

function writeResultsToCSV(results, callback) {
	var length = results.length;
	var writeStream = fileSystem.createWriteStream('./test.csv');
	var encoding = 'UTF-8';

	if (length > 0) {
		writeStream.write(buildCSVHeader(), encoding);
		
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

findPACESResults(function(err, results) {
	writeResultsToCSV(results, function() {
		console.log('Done');
	})
});