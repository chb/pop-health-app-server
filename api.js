const { Router } = require("express");
const moment     = require("moment");
const faker      = require("faker");
const db         = require("./db");
const lib        = require("./lib");
const auth       = require("./auth");
const presto     = require("./presto");
const router     = exports.router = Router({ mergeParams: true });

/**
 * Iterated over the time interval specified by @fromMoment and @toMoment and
 * calls the @callback once for each month.
 * @param {Moment} fromMoment The moment from which to start. Will be rounded to
 * the start of the month.
 * @param {Moment} toMoment The moment at which to end. Will be rounded to the
 * end of the month.
 * @param {(month: Moment)=> void} callback Called once for each month with a
 * moment pointing to the start of that month
 */
function forEachMonth(fromMoment, toMoment, callback) {
    const month = moment(fromMoment).startOf("month");
    while(month.isSameOrBefore(toMoment)) {
        callback(month);
        month.add(1, "months");
    }
}

/**
 * This will be called by the front-end to build it's UI structure. Should reply
 * with all the data that the UI needs for initializing itself.
 */
router.get("/ui", (req, res) => {
    res.json({
        dataSources  : db.dataSources,
        measures     : db.measures,
        organizations: db.organizations,
        payers       : db.payers
    });
});

/**
 * Get all the measures as defined in the database
 */
router.get("/measures", (req, res) => {
    res.json(db.measures);
});

/**
 * The following parameters are accepted:
 * - payer - The ID of the payer. Required!
 * - org - Organization ID. Use it multiple times for more than one org. If
 *   omitted, all organizations will be included in the response.
 * - clinic - Clinic ID. Use it multiple times for more than one clinic. If
 *   omitted, all clinics will be included in the response.
 * - ds - DataSource ID. Use it multiple times for more than one data source.
 *   If omitted, all data sources will be included in the response.
 * - startDate - The date from which to start computing the measurement values.
 *   Must be valid date string that `moment()` can parse. We compute results per
 *   month so the start date will be internally converted to point to the start
 *   of the month that it fits in. Defaults to the beginning of the previous year.
 * - endDate - The date at which to stop computing the measurement values.
 *   Must be valid date string that `moment()` can parse. We compute results per
 *   month so the start date will be internally converted to point to the end
 *   of the month that it fits in. Defaults to the end of the current month.
 */
router.get("/measures/results", auth.authenticate, (req, res) => {

    let { org, payer, clinic, ds, startDate, endDate } = req.query;

    // Payer -------------------------------------------------------------------
    if (!payer) {
        return res.status(400).json({ error: "payer parameter is required" });
    }
    if (!db.payers.find(p => p.id === payer)) {
        return res.status(400).json({ error: "payer parameter is not a valid payer ID" });
    }

    // Organizations -----------------------------------------------------------
    let organizations = lib.makeArray(org).filter(Boolean);
    if (!organizations.length) {
        organizations = db.organizations;
    } else {
        organizations = organizations.map(
            id => db.organizations.find(o => o.id === id)
        ).filter(Boolean);
        if (!organizations.length) {
            return res.status(400).json({ error: "no valid organization ID(s) provided" });
        }
    }

    // Clinics -----------------------------------------------------------------
    let clinics = lib.makeArray(clinic).filter(Boolean);
    if (!clinics.length) {
        clinics = db.clinics.map(o => o.id);
    } else {
        clinics = clinics.filter(
            id => db.clinics.find(o => o.id === id)
        );
    }

    // Data Sources ------------------------------------------------------------
    let dataSources = lib.makeArray(ds).filter(Boolean);
    if (!dataSources.length) {
        dataSources = db.dataSources.map(o => o.id);
    } else {
        dataSources = dataSources.filter(
            id => db.dataSources.find(o => o.id === id)
        );
    }

    // Start Date --------------------------------------------------------------
    startDate = moment(startDate + "");
    if (!startDate.isValid()) {
        startDate = moment().startOf("year").subtract(1, "year");
    }
    startDate = startDate.startOf("month");

    // End Date ----------------------------------------------------------------
    endDate = moment(endDate + "");
    if (!endDate.isValid()) {
        endDate = moment();
    }
    endDate = endDate.endOf("month");


    const out = {
        startDate,
        endDate,
        organizations: {}
    };

    // For every requested organization
    organizations.forEach(org => {
        const _org = {
            name: org.name,
            description: org.description,
            measures: []
        };

        // For every measure
        db.measures.forEach(measure => {
            const data = {};

            // For each month in the selected range
            forEachMonth(startDate, endDate, date => {
                data[date.format("YYYY-MM")] = Math.random() * 100;
            });

            _org.measures.push({
                id: measure.id,
                name: measure.name,
                data
            });
        });

        out.organizations[org.id] = _org;
    });

    res.json(out);
});

/**
 * The following parameters are accepted:
 * - payer  - The ID of the payer. Required!
 * - org    - Organization ID. Required!
 * - clinic - Clinic ID. Use it multiple times for more than one clinic. If
 *   omitted, all clinics will be included in the response.
 * - ds - DataSource ID. Use it multiple times for more than one data source.
 *   If omitted, all data sources will be included in the response.
 * - date - Required! The date for which to compute the measurement values.
 *   Must be valid date string that `moment()` can parse. We compute results per
 *   month so the date will be internally converted to point to the end of the
 *   month that it fits in.
 */
router.get("/measures/results/:measureId", auth.authenticate, (req, res) => {
    let { org, payer, clinic, ds, date } = req.query;

    // Payer -------------------------------------------------------------------
    if (!payer) {
        return res.status(400).json({ error: "payer parameter is required" });
    }
    if (!db.payers.find(p => p.id === payer)) {
        return res.status(400).json({ error: "payer parameter is not a valid payer ID" });
    }

    // Organizations -----------------------------------------------------------
    if (!org) {
        return res.status(400).json({ error: "org parameter is required" });
    }
    org = db.organizations.find(o => o.id === org);
    if (!org) {
        return res.status(400).json({ error: "org parameter is not a valid organization ID" });
    }

    // Clinic ------------------------------------------------------------------
    clinic = db.clinics.find(o => o.id === clinic);

    // Data Sources ------------------------------------------------------------
    let dataSources = lib.makeArray(ds).filter(Boolean);
    if (!dataSources.length) {
        dataSources = db.dataSources.map(o => o.id);
    } else {
        dataSources = dataSources.filter(
            id => db.dataSources.find(o => o.id === id)
        );
    }

    // Date --------------------------------------------------------------------
    date = moment(date + "");
    if (!date.isValid()) {
        date = moment().endOf("month");
    }
    date = date.endOf("month");

    // Measure -----------------------------------------------------------------
    const measure = db.measures.find(o => o.id === req.params.measureId);
    if (!measure) {
        return res.status(400).json({ error: `cannot find measure by id ("${req.params.measureId}")` });
    }

    // Result ------------------------------------------------------------------
    const result = presto.getResult({
        clinic_id: clinic ? clinic.id : null,
        dataset_id: dataSources,
        org_id: org,
        payer_id: payer,
        measure_id: req.params.measureId,
        month: date.format("MM"),
        year: date.format("YYYY")
    });

    const out = {
        id         : measure.id,
        name       : measure.name,
        description: measure.description,
        numerator  : measure.numerator,
        denominator: measure.denominator,
        date,
        organization: {
            name       : org.name,
            description: org.description
        },
        clinic,
        result: {
            numerator  : result.numerator,
            denominator: result.denominator,
            pct        : Math.round(result.numerator / result.denominator * 100)
        }
    };

    res.json(out);
});

router.get("/measures/report", auth.authenticate, (req, res) => {
    const type   = req.query.type   || "json";
    const offset = req.query.offset || 0;
    const limit  = req.query.limit  || 100;
    const header = [
        "Current Age",
        "Patient Name",
        "General PCP Name",
        "Measure Met",
        "Last Primary Care Appt Date",
        "Last Primary Care Appt Clinic"
    ];

    const data = [];

    for (let i = offset; i < offset + limit; i++) {
        data.push([
            // Current Age
            faker.random.number({ min: 1, max: 20 }),

            // Patient Name
            faker.name.firstName() + " " + faker.name.lastName(),

            // General PCP Name
            faker.name.firstName() + " " + faker.name.lastName(),

            // Measure Met
            faker.random.arrayElement(["Y", "N"]),

            // Last Primary Care Appointment Date
            moment(faker.date.recent(moment().dayOfYear())).format("MM/DD/YYYY"),

            // Last Primary Care Appointment Clinic
            faker.random.arrayElement(["PCL", "PCM"])
        ]);
    }

    if (type == "csv") {
        res.set({
            "Content-type": "text/csv",
            "Content-disposition": "attachment;filename=report.csv"
        });
        res.write(header.map(JSON.stringify).join(","));
        data.forEach(row => {
            res.write("\r\n" + row.map(JSON.stringify).join(","));
        });
        res.end();
    }
    else {
        res.json({
            header,
            offset,
            limit,
            data
        });
    }
});

// function getMeasureResults(measure, filter = {})
// {
//     const {
//         organization,
//         payer,
//         clinic
//     } = filter;
// }