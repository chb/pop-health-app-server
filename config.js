module.exports = {
    host: process.env.HOST || "0.0.0.0",
    port: process.env.PORT || "3003",

    // Our hard-coded list of user (this is good enough for our demo!)
    users: [
        {
            username: "user@aco.org",
            password: "password"
        },
        {
            username: "user@payer.org",
            password: "password"
        }
    ]
};
