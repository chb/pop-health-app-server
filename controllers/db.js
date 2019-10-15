const sqlite3 = require("sqlite3");
const debug   = require("debug")("DB");

const DB = new sqlite3.Database(`${__dirname}/../database.db`);

/**
 * Calls database methods and returns a promise
 * @param {String} method
 * @param {*[]} args
 */
DB.promise = (...args) =>
{
    let [method, ...params] = args;
    return new Promise((resolve, reject) => {
        const logParams = [...params];
        debug("SQL: ", logParams.shift(), "\nparams: ", logParams);
        DB[method](...params, (error, result) => {
            if (error) {
                debug(error);
                return reject(error);
            }
            debug(result);
            resolve(result);
        });
    });
};

module.exports = DB;
