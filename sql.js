const { Router } = require("express");
// const lento      = require("lento");
const bodyParser = require("body-parser");
const auth       = require("./controllers/auth");
const csvWriter  = require("csv-write-stream");
const pipeline   = require("readable-stream").pipeline;
const through2   = require("through2");
const pool       = require("./dbPool");

const router = exports.router = Router({ mergeParams: true });

function createRowStream(sql)
{
    return pool.getConnection().then(connection => {
        let stream = connection.connection.query(sql).stream();
        stream.once("close", () => connection.release());
        return stream;
    });
}

// const client = lento({
//     user    : "presto",
//     hostname: "34.74.56.14",
//     catalog : "hive",
//     schema  : "leap"
// });


router.get("/csv", auth.authenticate, async (req, res) => {
    let query = req.query.q || "";
    if (!query) {
        return res.status(400).json({ error: "A 'q' parameter is required" }).end();
    }
    console.log(query);
    // query = query.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
    query = query.replace(/-/g, "+").replace(/_/g, "/");
    console.log(query);
    query = Buffer.from(query, "base64").toString("utf8");
    console.log(query);

    // let source = client.createRowStream(query);
    let source = await createRowStream(query);

    res.set({
        "Content-type"       : "text/plain",
        "Content-disposition": "attachment;filename=report.csv"
    });

    pipeline(
        source,
        csvWriter(),
        res,
        (err) => {
            console.error(err);
        }
    );
});

router.post("/", auth.authenticate, bodyParser.urlencoded({ extended: true }), async (req, res) => {

    let source = await createRowStream(req.body.query);
    let header;
    let data = [];
    let len = 0;
    const maxRows = 1000;


    source.pipe(through2.obj(function(row, enc, next) {
        if (len < maxRows) {
            if (!header) {
                header = Object.keys(row);
            }
            len = data.push(Object.values(row));

            // if (len >= maxRows) {
            //     source.destroy();
            // }
        }
        if (len >= maxRows) {
            source.destroy();
        }
        next();
    }));

    source.on("close", () => {
        if (!res.headersSent) {
            res.json({ header, data });
        }
    });

    source.on("end", () => {
        if (!res.headersSent) {
            res.json({ header, data });
        }
    });

    source.on("error", e => {
        res.status(400).json({ error: e.message }).end();
    });
});