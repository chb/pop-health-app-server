const mysql    = require("mysql2/promise");

module.exports = mysql.createPool({
    host              : "34.73.199.182",
    user              : "bulk-data-user",
    password          : "^$_EgVtYJ^mb&5W_",
    database          : "bulk-data-stu-3",
    waitForConnections: true,
    connectionLimit   : 10,
    queueLimit        : 0
});
