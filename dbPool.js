const mysql    = require("mysql2/promise");

module.exports = mysql.createPool({
    host              : "34.73.199.182",
    user              : "bulk-data-read-only-user",
    password          : "}229$[2h7#gm%.RZoZ2dNscAS$]f",
    database          : "bulk-data-stu-3",
    waitForConnections: true,
    connectionLimit   : 10,
    queueLimit        : 0
});
