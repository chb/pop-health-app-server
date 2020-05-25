const mysql = require("mysql2/promise");

// We connect to MySQL database that is hosted on Google Cloud and is publicly
// accessible in read-only mode
module.exports = mysql.createPool({
    host              : "35.221.49.252",
    user              : "bulk-data-read-only-user",
    password          : "nq9FkLs}btq]mP8ZW8}c4ke)",
    database          : "bulk-data-stu-3",
    waitForConnections: true,
    connectionLimit   : 10,
    queueLimit        : 0
});