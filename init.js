'use strict'; // eslint-disable-line strict
require('babel-core/register');

const assert = require('assert');
const buffer = require('buffer');
const fs = require('fs');
const os = require('os');

const async = require('async');
const ioctl = require('ioctl');

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
const dataPathFD = fs.openSync(dataPath, 'r');
if (os.type() === 'Linux') {
    const buffer = new Buffer(8);
    const GETFLAGS = 2148034049;
    const SETFLAGS = 1074292226;
    const FS_DIRSYNC_FL = 65536;
    const status = ioctl(dataPathFD, GETFLAGS, buffer);
    console.log("status!!", status);
} else {
    logger.warn('WARNING: Synchronization directory updates are not ' +
        'supported on this platform. Newly written data could be lost ' +
        'if your system crashes before the operating system is able to ' +
        'write directory updates.');
}

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
