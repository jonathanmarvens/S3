'use strict'; // eslint-disable-line strict
require('babel-core/register');

const assert = require('assert');
const fs = require('fs');

const async = require('async');

const constants = require('./constants').default;
const config = require('./lib/Config.js').default;
const logger = require('./lib/utilities/logger.js').logger;


if (config.backends.data !== 'file' && config.backends.metadata !== 'file') {
    logger.info('No init required. Go forth and store data.');
    process.exit(0);
}

const dataPath = config.filePaths.dataPath;
const metadataPath = config.filePaths.metadataPath;

fs.accessSync(dataPath, fs.F_OK | fs.R_OK | fs.W_OK);
fs.accessSync(metadataPath, fs.F_OK | fs.R_OK | fs.W_OK);

// TODO: ioctl on the data and metadata directories fd,
// with params FS_IOC_SETFLAGS and FS_DIRSYNC_FL


// Create 3511 subdirectories for the data file backend
const subDirs = Array.from({ length: constants.folderHash },
    (v, k) => (k + 1).toString());
async.eachSeries(subDirs, (subDirName, next) => {
    fs.mkdir(`${dataPath}/${subDirName}`, err => {
        // If already exists, move on
        if (err && err.errno !== -17) {
            return next(err);
        }
        return next();
    });
},
 err => {
     assert.strictEqual(err, null, `Error creating data files ${err}`);
     logger.info('Init complete.  Go forth and store data.');
 });