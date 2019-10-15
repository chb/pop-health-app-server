const { Router } = require("express");
const lento      = require("lento");
const bodyParser = require("body-parser");
const auth       = require("./controllers/auth");
const csvWriter  = require("csv-write-stream");
const pipeline   = require("readable-stream").pipeline;
const through2   = require("through2");

const router = exports.router = Router({ mergeParams: true });

const client = lento({
    user    : "presto",
    hostname: "34.74.56.14",
    catalog : "hive",
    schema  : "leap"
});


router.get("/csv", auth.authenticate, (req, res) => {
    let query = req.query.q || "";
    if (!query) {
        return res.status(400).json({ error: "A 'q' parameter is required" }).end();
    }

    query = Buffer.from(query, "base64").toString("utf8");

    let source = client.createRowStream(query);

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

router.post("/", auth.authenticate, bodyParser.urlencoded({ extended: true }), (req, res) => {

    let source = client.createRowStream(req.body.query, {
        // pageSize: 300,
        // rowFormat: "array"
    });

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