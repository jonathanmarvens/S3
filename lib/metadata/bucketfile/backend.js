import net from 'net';
import fs from 'fs';
import cluster from 'cluster';
import events from 'events';
import assert from 'assert';

import level from 'level';
import multilevel from 'multilevel';
import sublevel from 'level-sublevel';
import arsenal from 'arsenal';

import { logger } from '../../utilities/logger';
import BucketInfo from '../BucketInfo';
import constants from '../../../constants';
import config from '../../Config';

const errors = arsenal.errors;

const METADATA_PORT = 9990;
const METADATA_PATH = `${config.filePaths.metadataPath}/`;
const MANIFEST_JSON = 'manifest.json';
const MANIFEST_JSON_TMP = 'manifest.json.tmp';
const ROOT_DB = 'rootDB';
const METASTORE = '__metastore';
const OPTIONS = { sync: true };

class BucketFileInterface {

    constructor() {
        this.logger = logger;
        if (cluster.isMaster) {
            this.startServer();
        } else {
            this.refcnt = 0;
            this.reConnect();
            this.notifier = new events.EventEmitter();
        }
    }

    /**
     * Start the server
     * @return {undefined}
     */
    startServer() {
        const rootDB = level(METADATA_PATH + ROOT_DB);
        const sub = sublevel(rootDB);
        sub.methods = sub.methods || {};
        sub.methods.createSub = { type: 'async' };
        sub.createSub = (subName, cb) => {
            try {
                sub.sublevel(subName);
                multilevel.writeManifest(sub,
                                         METADATA_PATH +
                                         MANIFEST_JSON_TMP);
                fs.renameSync(METADATA_PATH + MANIFEST_JSON_TMP,
                              METADATA_PATH + MANIFEST_JSON);
                cb();
            } catch (err) {
                cb(err);
            }
        };
        const metastore = sub.sublevel(METASTORE);
        /* due to a chicken-and-egg issue we need to create
           usersBucket here */
        sub.sublevel(constants.usersBucket);
        const usersBucketAttr = new BucketInfo(constants.usersBucket,
                                               'admin', 'admin',
                                               new Date().toJSON(), 2);
        metastore.put(constants.usersBucket, usersBucketAttr.serialize());
        const stream = metastore.createKeyStream();
        stream
            .on('data', e => {
                // automatically recreate existing sublevels
                sub.sublevel(e);
            })
            .on('error', err => {
                this.logger.fatal('error listing metastore', { error: err });
                throw (errors.InternalError);
            })
            .on('end', () => {
                multilevel.writeManifest(sub, METADATA_PATH + MANIFEST_JSON);
                this.logger.info('starting metadata file backend server');
                net.createServer(con => {
                    con.pipe(multilevel.server(sub)).pipe(con);
                }).listen(METADATA_PORT);
            });
    }

    /**
     * Reconnect to the server
     * @return {undefined}
     */
    realReConnect() {
        if (this.client !== undefined) {
            this.client.close();
        }
        delete require.cache[require.resolve(METADATA_PATH + MANIFEST_JSON)];
        const manifest = require(METADATA_PATH + MANIFEST_JSON);
        this.client = multilevel.client(manifest);
        const con = net.connect(METADATA_PORT);
        con.pipe(this.client.createRpcStream()).pipe(con);
        this.metastore = this.client.sublevel(METASTORE);
    }

    /**
     * Reconnect to the server while possibly waiting for
     * it to be idle
     * @return {undefined}
     */
    reConnect() {
        if (this.refcnt === 0) {
            this.realReConnect();
        } else {
            this.notifier.on('idle', () => {
                this.realReConnect();
            });
        }
    }

    /**
     * Take a reference on the client
     * @return {undefined}
     */
    ref() {
        this.refcnt++;
    }

    /**
     * Unreference the client
     * @return {undefined}
     */
    unRef() {
        this.refcnt--;
        assert(this.refcnt >= 0);
        if (this.refcnt === 0) {
            this.notifier.emit('idle');
        }
    }

    /**
     * Check DB existence
     * @param {String} bucketName - name of bucket
     * @param {Object} log - logger
     * @param {function} cb - callback(err,exists)
     * @return {undefined}
     */
    checkDbExists(bucketName, log, cb) {
        this.metastore.get(bucketName, err => {
            if (err) {
                if (err.notFound) {
                    return cb(null, false);
                }
                log.error('error checking db existence', { error: err });
                return cb(errors.InternalError, null);
            }
            return cb(null, true);
        });
    }

    /**
     * Load DB if exists
     * @param {String} bucketName - name of bucket
     * @param {Object} log - logger
     * @param {function} cb - callback(err, db)
     * @return {undefined}
     */
    loadDBIfExists(bucketName, log, cb) {
        this.checkDbExists(bucketName, log, (err, exists) => {
            if (err) {
                return cb(err);
            }
            if (exists) {
                let db;
                try {
                    db = this.client.sublevel(bucketName);
                } catch (err) {
                    this.reConnect();
                    db = this.client.sublevel(bucketName);
                }
                this.ref();
                return cb(null, db);
            }
            return cb(errors.NoSuchBucket, null);
        });
        return undefined;
    }

    createBucket(bucketName, bucketMD, log, cb) {
        this.checkDbExists(bucketName, log, (err, exists) => {
            if (err) {
                return cb(err);
            }
            if (exists) {
                return cb(errors.BucketAlreadyExists);
            }
            this.client.createSub(bucketName, err => {
                if (err) {
                    log.error('error creating sublevel', { error: err });
                    return cb(errors.InternalError);
                }
                this.putBucketAttributes(bucketName,
                                         bucketMD, log, cb);
                return undefined;
            });
            return undefined;
        });
        return undefined;
    }

    getBucketAttributes(bucketName, log, cb) {
        this.metastore.get(bucketName, (err, data) => {
            if (err) {
                return cb(errors.NoSuchBucket);
            }
            return cb(null, BucketInfo.deSerialize(data));
        });
        return undefined;
    }

    getBucketAndObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucketAttr) => {
            if (err) {
                return cb(err);
            }
            this.loadDBIfExists(bucketName, log, (err, db) => {
                if (err) {
                    return cb(err);
                }
                db.get(objName, (err, objAttr) => {
                    this.unRef();
                    if (err) {
                        if (err.notFound) {
                            return cb(null, {
                                bucket: bucketAttr.serialize(),
                            });
                        }
                        log.error('error getting object', { error: err });
                        return cb(errors.InternalError);
                    }
                    return cb(null, {
                        bucket: bucketAttr.serialize(),
                        obj: objAttr,
                    });
                });
                return undefined;
            });
            return undefined;
        });
        return undefined;
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        this.metastore.put(bucketName, bucketMD.serialize(), OPTIONS,
                           err => {
                               if (err) {
                                   log.error('error putting attributes',
                                             { error: err });
                                   return cb(errors.InternalError);
                               }
                               return cb();
                           });
        return undefined;
    }

    deleteBucket(bucketName, log, cb) {
        // we could remove bucket from manifest but it is not a problem
        this.metastore.del(bucketName,
                           err => {
                               if (err) {
                                   log.error('error deleting bucket',
                                             { error: err });
                                   return cb(errors.InternalError);
                               }
                               return cb();
                           });
        return undefined;
    }

    putObject(bucketName, objName, objVal, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.put(objName, JSON.stringify(objVal),
                   OPTIONS, err => {
                       this.unRef();
                       if (err) {
                           log.error('error putting object',
                                     { error: err });
                           return cb(errors.InternalError);
                       }
                       return cb();
                   });
            return undefined;
        });
    }

    getObject(bucketName, objName, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.get(objName, (err, data) => {
                this.unRef();
                if (err) {
                    if (err.notFound) {
                        return cb(errors.NoSuchObject);
                    }
                    log.error('error getting object',
                              { error: err });
                    return cb(errors.InternalError);
                }
                return cb(null, JSON.parse(data));
            });
            return undefined;
        });
    }

    deleteObject(bucketName, objName, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.del(objName, OPTIONS, err => {
                this.unRef();
                if (err) {
                    log.error('error deleting object',
                              { error: err });
                    return cb(errors.InternalError);
                }
                return cb();
            });
            return undefined;
        });
    }

    /**
     *  This function checks if params have a property name
     *  If there is add it to the finalParams
     *  Else do nothing
     *  @param {String} name - The parameter name
     *  @param {Object} params - The params to search
     *  @param {Object} extParams - The params sent to the extension
     *  @return {undefined}
     */
    addExtensionParam(name, params, extParams) {
        if (params.hasOwnProperty(name)) {
            // eslint-disable-next-line no-param-reassign
            extParams[name] = params[name];
        }
    }

    /**
     * Used for advancing the last character of a string for setting upper/lower
     * bounds
     * For e.g., _setCharAt('demo1') results in 'demo2',
     * _setCharAt('scality') results in 'scalitz'
     * @param {String} str - string to be advanced
     * @return {String} - modified string
     */
    _setCharAt(str) {
        let chr = str.charCodeAt(str.length - 1);
        chr = String.fromCharCode(chr + 1);
        return str.substr(0, str.length - 1) + chr;
    }

    /**
     *  This complex function deals with different extensions of bucket listing:
     *  Delimiter based search or MPU based search.
     *  @param {String} bucketName - The name of the bucket to list
     *  @param {Object} params - The params to search
     *  @param {Object} log - The logger object
     *  @param {function} cb - Callback when done
     *  @return {undefined}
     */
    internalListObject(bucketName, params, log, cb) {
        const requestParams = {};
        let Ext;
        const extParams = {};
        // multipart upload listing
        if (params.listingType === 'multipartuploads') {
            Ext = arsenal.listMPU.ListMultipartUploads;
            this.addExtensionParam('queryPrefixLength', params, extParams);
            this.addExtensionParam('splitter', params, extParams);
            if (params.keyMarker) {
                requestParams.gt = `overview${params.splitter}` +
                    `${params.keyMarker}${params.splitter}`;
                if (params.uploadIdMarker) {
                    requestParams.gt += `${params.uploadIdMarker}`;
                }
                // advance so that lower bound does not include the supplied
                // markers
                requestParams.gt = this._setCharAt(requestParams.gt);
            }
        } else {
            Ext = arsenal.delimiter.Delimiter;
            if (params.marker) {
                requestParams.gt = params.marker;
                this.addExtensionParam('gt', requestParams, extParams);
            }
        }
        this.addExtensionParam('delimiter', params, extParams);
        this.addExtensionParam('maxKeys', params, extParams);
        if (params.prefix) {
            requestParams.start = params.prefix;
            requestParams.lt = this._setCharAt(params.prefix);
            this.addExtensionParam('start', requestParams, extParams);
        }
        const extension = new Ext(extParams, log);
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            let cbDone = false;
            const stream = db.createReadStream(requestParams);
            stream
                .on('data', e => {
                    if (extension.filter(e) === false) {
                        stream.emit('end');
                        stream.destroy();
                    }
                })
                .on('error', err => {
                    if (!cbDone) {
                        this.unRef();
                        cbDone = true;
                        log.error('error listing objects',
                                  { error: err });
                        cb(errors.InternalError);
                    }
                })
                .on('end', () => {
                    if (!cbDone) {
                        this.unRef();
                        cbDone = true;
                        const data = extension.result();
                        cb(null, data);
                    }
                });
            return undefined;
        });
    }

    listObject(bucketName, params, log, cb) {
        return this.internalListObject(bucketName, params, log, cb);
    }

    listMultipartUploads(bucketName, params, log, cb) {
        return this.internalListObject(bucketName, params, log, cb);
    }
}

export default BucketFileInterface;