var _ = require('lodash');
var async = require('async');

/**
 * Inserts new records into the database.
 *
 * @param {QueryRunner} queryRunner
 *   The query runner. This can be a plain mysql connection instance or an adapter that 'duck types' the QueryRunner.
 */
var InsertRecordsHelper = function(queryRunner) {

    var self = this;

    this.queryRunner = queryRunner;

    /**
     * Maintains information about inserted data, which can be used elsewhere.
     * @type {{object}}
     */
    this.data = {};

    /**
     * Inserts the specified data.
     * @param {object} content
     *   The content object which describes the records to be inserted.
     *   Contains data per object type (=table name), with local identifiers as keys and other parameters as array.
     *   After inserting, the data array will be filled with the same structure, but instead of a parameter array the inserted
     *   id for later reference.
     *
     *   A parameter may be a reference object: ref("TABLE_NAME", "ID"), for example ref("SecurityContent", "developer")
     *   gives the record id of the previously inserted security context locally identified by "ID".
     *   Note that the property order is important here: the referenced object must be created before the referencing object.
     * @param {Function} callback
     */
    this.insert = function(content, callback) {
        var tasks = [];
        for (var table in content) {
            if (content.hasOwnProperty(table)) {
                (function(table) {
                    tasks.push(insertObjects.bind(self, content[table], table));
                })(table);
            }
        }
        async.series(tasks, callback);
    };

    /**
     * Inserts all objects of the specified type and sets .
     * @param data
     * @param table
     * @param cb
     */
    var insertObjects = function(data, table, cb) {
        var tasks = [];

        var options = data.hasOwnProperty('__options') ? data.__options : {};

        var idColumn = (options.existing && options.existing.idColumn) ? options.existing.idColumn : "id";

        // Get the db ref getter for matching specified records with existing records.
        var getExistingRecord = null;
        if (options.existing && options.existing.refColumns) {
            var temp = self.getDbRefGetter(table, options.existing.refColumns, idColumn);
            getExistingRecord = function(record, cb) {
                // Get ref values from record object.
                var refValues = _.values(_.pick(record, options.existing.refColumns));

                // Request id from the database.
                return temp(refValues).getId(function(err, id) {
                    if (err) {
                        if (err.name == "notfound") {
                            return cb(null, null);
                        } else {
                            return cb(err);
                        }
                    }

                    cb(null, id);
                });
            };
        }

        // Initialize local id array.
        if (!self.data.hasOwnProperty(table)) {
            self.data[table] = {};
        }

        /**
         * Handles a single record from the content object.
         * @param table
         * @param id
         * @param item
         */
        var handleRecord = function(table, id, item, cb) {
            // Convert references in params.
            self.convertRefs(item, function() {
                if (getExistingRecord) {
                    getExistingRecord(item, function(err, dbId) {
                        if (err) {
                            return cb(err);
                        }
                        if (dbId) {
                            // Set local reference.
                            self.data[table][id] = dbId;

                            if (options.existing.update) {
                                // Update.
                                self.databaseUpdate(table, idColumn, dbId, item, cb);
                            } else if (options.existing.replace) {
                                // Replace.
                                self.databaseReplace(table, idColumn, dbId, item, cb);
                            }
                        } else {
                            // Insert.
                            self.databaseInsert(table, item, function(err, insertId) {
                                self.data[table][id] = insertId;
                                cb(err);
                            });
                        }
                    });
                } else {
                    // Insert.
                    self.databaseInsert(table, item, function(err, insertId) {
                        self.data[table][id] = insertId;
                        cb(err);
                    });
                }
            });
        };

        for (var id in data) {
            if (data.hasOwnProperty(id) && (id != "__options")) {
                // Add insert record task.
                (function(table, id, item) {
                    tasks.push(function (cb) {
                        handleRecord(table, id, item, cb);
                    });
                })(table, id, data[id]);
            }
        }

        // Perform inserts for this object type.
        async.series(tasks, cb);
    };

    /**
     * An update call.
     * @param table
     *   Content type of the object. This must be the table name.
     * @param idColumn
     *   The column that contains the id.
     * @param id
     *   The id value.
     * @param {object} params
     *   Parameters to be inserted (hashmap).
     * @param cb
     */
    this.databaseUpdate = function(table, idColumn, id, params, cb) {
        var columns = _.keys(params);
        var values = _.values(params);

        var query = "UPDATE `" + table + "` SET `" + columns.join('` = ?, `') + "` = ? WHERE `" + idColumn + "` = ?";

        queryRunner.query(query, values.concat([id]), cb);
    };

    /**
     * An insert call.
     * @param table
     *   Content type of the object. This must be the table name.
     * @param {object} params
     *   Parameters to be inserted (hashmap).
     * @param cb
     */
    this.databaseInsert = function(table, params, cb) {
        var columns = _.keys(params);
        var values = _.values(params);

        var query = "INSERT INTO `" + table + "` (`" + columns.join("`,`") + "`) VALUES (" + columns.map(function(item) {return "?"}).join(",") + ")";
        queryRunner.query(query, values, function(err, res) {
            cb(err, res ? res.insertId : null);
        });
    };

    /**
     * A replace call.
     * @param table
     *   Content type of the object. This must be the table name.
     * @param idColumn
     *   The column that contains the id.
     * @param id
     *   The id value.
     * @param {object} params
     *   Parameters to be inserted (hashmap).
     * @param cb
     */
    this.databaseReplace = function(table, idColumn, id, params, cb) {
        var columns = _.keys(params);
        var values = _.values(params);

        columns.push(idColumn);
        values.push(id);

        queryRunner.query("REPLACE INTO `" + table + "` (`" + columns.join("`,`") + "`) VALUES (" + columns.map(function(item) {return "?"}).join(",") + ")", values, cb);
    };

    /**
     * In the specified parameters object, all LocalReference and DbReference items are converted to the id.
     * @param {Object} params
     * @param cb
     * @throws String
     *   In case one of the local references could not be converted.
     */
    this.convertRefs = function(params, cb) {
        var tasks = [];
        for (var key in params) {
            if (params.hasOwnProperty(key)) {
                if (params[key] instanceof LocalReference) {
                    try {
                        params[key] = convertRef(params[key]);
                    } catch(e) {
                        console.log(e);
                        console.log("using null value for " + key);
                        params[key] = null;
                    }
                } else if (params[key] instanceof DbReference) {
                    (function(key) {
                        tasks.push(function(cb) {
                            params[key].getId(function(err, id) {
                                if (err) {
                                    console.log(err);
                                    console.log("using null value for " + key);
                                }
                                params[key] = id;
                                cb(null);
                            });
                        });
                    })(key);
                } else if (_.isPlainObject(params[key]) || _.isArray(params[key])) {
                    // Resolve recursively
                    (function(key) {
                        tasks.push(function(cb) {
                            self.convertRefs(params[key], function(err) {
                                if (err) {
                                    return cb(err);
                                }
                                cb(null);
                            });
                        });
                    })(key);
                }
            }
        }
        if (tasks.length > 0) {
            async.series(tasks, cb);
        } else {
            cb(null);
        }
    };

    /**
     * Converts the specified parameter.
     * @param {LocalReference} param
     * @return {Number}
     *   The id, or null if the id couldn't be found.
     *  @throws String
     *    In case the reference could not be found.
     */
    var convertRef = function(param) {
        return self.getReferenced(param.table, param.id);
    };

    /**
     * Returns the insert id of the previously inserted object.
     * @param table
     *   The table name / content type.
     * @param id
     *   The local id, as was specified in the content array.
     * @return {Number}
     *   The insert id.
     *  @throws String
     *    In case the reference could not be found.
     */
    this.getReferenced = function(table, id) {
        if (this.data[table] && this.data[table][id]) {
            return this.data[table][id];
        } else {
            throw "reference '" + table + ":" + id + "' could not be found";
        }
    };

    /**
     * Returns a function that accepts an array with values for refColumns and returns the id value for it.
     * @param table
     *   The table name.
     * @param {String[]} [refColumns]
     *   Reference column names. The values supplied to the getter should be in the same order. Default is ["name"].
     * @param {String} [idColumn]
     *   The id column name. Default is "id". An array can be used in order to return a composite key.
     * @return {Function}
     *   A function that produces parameters that may be used in a content object.
     */
    this.getDbRefGetter = function(table, refColumns, idColumn) {
        if (!refColumns) {
            refColumns = ['name'];
        }
        if (!idColumn) {
            idColumn = 'id';
        }

        var idColumns;
        if (idColumn instanceof Array) {
            idColumns = idColumn;
        } else {
            idColumns = [idColumn];
        }

        var query = "SELECT `" + idColumns.join("`,`") + "` FROM `" + table + "` WHERE ";
        for (var i = 0; i < refColumns.length; i++) {
            if (i > 0) {
                query += " AND ";
            }
            query += "`" + refColumns[i] + "` = ?"
        }
        return function(refValues) {
            if (!(refValues instanceof Array)) {
                refValues = [refValues];
            }
            return new DbReference(self.queryRunner, table, refValues, query);
        };
    };

    /**
     * Returns a deleter function that accepts an array with values for refColumns and deletes the record(s) with that reference.
     * @param table
     *   The table name.
     * @param {String[]} [refColumns]
     *   Reference column names. The values supplied to the deleter should be in the same order. Default is ["name"].
     * @return {Function}
     *   A function that deletes all records with the specified values for refColumns.
     */
    this.getDbRefDeleter = function(table, refColumns) {
        if (!refColumns) {
            refColumns = ['name'];
        }

        if (refColumns.length == 0) {
            throw "Specify at least one column.";
        }

        var query = "DELETE FROM `" + table + "` WHERE ";
        for (var i = 0; i < refColumns.length; i++) {
            if (i > 0) {
                query += " AND ";
            }
            query += "`" + refColumns[i] + "` = ?"
        }
        return function(refValues, cb) {
            if (!(refValues instanceof Array)) {
                refValues = [refValues];
            }
            self.queryRunner.query(query, refValues, cb);
        };
    };

    /**
     * Adds a parameter reference to a previously inserted object.
     * @param table
     *   The content type.
     * @param id
     *   The local id, as specified in the content object.
     * @returns {LocalReference}
     */
    this.ref = function(table, id) {
        return new LocalReference(table, id);
    };

    /**
     * Resolves an object with db references.
     * @param {object} items
     * @param cb
     */
    this.getRefs = function(items, cb) {
        var data = {};
        var tasks = [];
        for (var key in items) {
            (function(key) {
                tasks.push(function(cb) {
                    items[key].getId(function(err, id) {
                        data[key] = id;
                        cb(err);
                    });
                });
            })(key);
        }

        async.parallel(tasks, function(err) {
            if (err) {
                cb(err);
            } else {
                cb(null, data);
            }
        });
    };

};

/**
 * A local reference.
 */
var LocalReference = function(table, id) {
    this.table = table;
    this.id = id;
};

/**
 * A reference that must be loaded from the database.
 */
var DbReference = function(queryRunner, table, refValues, query) {
    var self = this;

    this.queryRunner = queryRunner;
    this.table = table;
    this.query = query;
    this.refValues = refValues;

    self.getId = function(cb) {
        var id;
        var refTasks = [];
        for (var i = 0; i < self.refValues.length; i++) {
            if (self.refValues[i] instanceof DbReference) {
                (function(i) {
                    // Recursive db reference.
                    refTasks.push(function (cb) {
                        self.refValues[i].getId(function(err, id) {
                            if (err) {
                                cb(err);
                            } else {
                                self.refValues[i] = id;
                                cb(null);
                            }
                        })
                    });
                })(i);
            }
        }
        var tasks = [];
        if (refTasks.length > 0) {
            tasks.push(function(cb) {
                async.parallel(refTasks, function(err) {
                    cb(err);
                });
            });
        }
        tasks.push(function(cb) {
            self.queryRunner.query(query, refValues, function(err, res) {
                if (err || !res || !res[0]) {
                    cb("reference '" + self.table + ":" + refValues.join(',') + "' could not be found");
                } else {
                    var fields = _.values(res[0]);
                    if (fields.length == 1) {
                        cb(null, fields[0]);
                    } else {
                        cb(null, res[0]);
                    }
                }
            });
        });
        async.waterfall(tasks, cb);
    };
};

/**
 * Specifies the QueryRunner (adapter pattern).
 * No need to extend, just use duck typing.
 * @abstract
 */
var QueryRunner = function() {

    /**
     * Runs the SQL query
     * @param query
     * @param parameters
     * @param cb
     *   Called with err and res, res is an array of objects as returned by the mysql module.
     */
    this.query = function(query, parameters, cb) {
        // This is just a stub that is meant to describe the implementation.
    }

};

module.exports = InsertRecordsHelper;
