const express      = require("express");
const cookieParser = require("cookie-parser");
const cors         = require("cors");
const config       = require("./config");
const auth         = require("./auth");
const api          = require("./api");

const app = express();

app.use(cors({ origin: true, credentials: true }));
app.use(cookieParser());
app.use(auth.authenticate);
app.use("/auth", auth.router);
app.use("/api", api.router);

app.listen(config.port, config.host, () => {
    console.log(`Server listening at ${config.host}:${config.port}`);
});
