const DB       = require("./db");
const lento    = require("lento");
const through2 = require("through2");
const moment   = require("moment");
const lib      = require("../lib");
const pool     = require("../dbPool");

// The data that we use does have some tags already. We could use FHIR tags to
// mark the data depending on where it came from. For the purpose of this app
// we just re-use the tags that ere already there.
const TAG_MAP = {
    BCH       : "smart-7-2017",
    PO        : "synthea-7-2017",
    PPOC      : "pro-7-2017",
    BCH_EPIC  : "bch-360",
    BCH_CERNER: "",
};

const client = lento({
    user    : "presto",
    hostname: "34.74.56.14",
    catalog : "hive",
    schema  : "leap"
});



class MeasureResult
{
    constructor(id)
    {
        this.id = id;
    }

    static async syncHypertension(year, orgId, dsId)
    {
        const bpCode = "55284-4";
        const hypertensionCodes = [
            "1201005", // Benign essential hypertension
            "38341003" // Essential hypertension
        ];
        const startDate = `${year}-01-01`;
        const endDate   = `${year}-12-31`;

        // Select the minimum information that we can work with - patient ID,
        // vaccine code and vaccine date
        let sql = "SELECT ";

        // Average systolic BP per patient per month
        sql += "AVG(o.resource_json ->> '$.component[0].valueQuantity.value') AS systolic, ";

        // Average diastolic BP per patient per month
        sql += "AVG(o.resource_json ->> '$.component[1].valueQuantity.value') AS diastolic, ";

        // The patient ID of each group
        sql += "SUBSTRING_INDEX(o.resource_json ->> '$.subject.reference', '/', -1) AS patient, ";

        // The month of each group
        sql += "MONTH(o.resource_json ->> '$.effectiveDateTime') AS `month` ";

        // Select from observations
        sql += "FROM Observation o WHERE ";

        // Only use BP observations
        sql += `o.resource_json ->> '$.code.coding[0].code' = '${bpCode}' `;

        // The observation must be taken after (or at) the beginning of the year
        sql += `AND DATEDIFF(o.resource_json ->> '$.effectiveDateTime', '${startDate}') >= 0 `;

        // The observation must be taken before (or at) the end of the year
        sql += `AND DATEDIFF(o.resource_json ->> '$.effectiveDateTime', '${endDate}') <= 365 `;

        // The observation must be for patients between 18 and 64 with HTN
        sql += "AND SUBSTRING_INDEX(o.resource_json ->> '$.subject.reference', '/', -1) IN (" +
               "SELECT p.resource_id FROM Patient AS p JOIN `Condition` AS c ON (" +
               "c.resource_json ->> '$.subject.reference' = CONCAT('Patient/', p.resource_id)) " +
               `WHERE c.resource_json ->> '$.code.coding[0].code' IN ('${hypertensionCodes.join("', '")}') ` +
               `AND DATE_ADD(p.resource_json ->> '$.birthDate', INTERVAL 18 YEAR) <= DATE('${startDate}')` +
               `AND DATE_ADD(p.resource_json ->> '$.birthDate', INTERVAL 64 YEAR) >= DATE('${endDate}')) `;

        // Group by patient and month to compute the averages
        sql += "GROUP BY patient, `month`";

        console.log(sql);

        const [rows] = await pool.query(sql);

        let DENOMINATOR = rows.length;

        // For each month, count how many patients have average BP below the thresholds?
        // We start by assuming that all patients are OK, and then subtract those
        // for whom we have high BP data.
        let months = [
            DENOMINATOR, DENOMINATOR, DENOMINATOR, DENOMINATOR,
            DENOMINATOR, DENOMINATOR, DENOMINATOR, DENOMINATOR,
            DENOMINATOR, DENOMINATOR, DENOMINATOR, DENOMINATOR
        ];

        rows.forEach(row => {

            // Increment this for every patient
            DENOMINATOR += 1;

            // Increment the month value for each patient who does NOT have
            // average hypertension during that month
            if (row.systolic > 140 || row.diastolic > 90) {
                months[row.month - 1] -= 1;
            }
        });

        console.log(months);

        const tasks = months.map((numerator, index) => DB.promise(
            "run",
            "INSERT OR REPLACE INTO measure_results_2 (" +
                "measure_id, date, numerator, denominator, org_id, ds_id" +
            ") VALUES (?, ?, ?, ?, ?, ?)",
            [
                "controlling_high_blood_pressure",
                `${year}-${index + 1 < 10 ? "0" + (index + 1) : index + 1 }-01`,
                numerator,
                DENOMINATOR,
                orgId,
                dsId
            ]
        ));

        await Promise.all(tasks);
    }

    static async syncImmunizationsForAdolescents(year, orgId, dsId)
    {
        const vaccineCodes = [
            "62",  // HPV, quadrivalent
            "114", // meningococcal MCV4P
            "115"  // Tdap
        ];
        const startDate = `${year}-01-01`;
        const endDate   = `${year}-12-31`;

        // Select the minimum information that we can work with - patient ID,
        // vaccine code and vaccine date
        let sql = `SELECT
            p.resource_json ->> '$.id'         AS \`id\`,
            i.code                             AS \`vaccineCode\`,
            date(i.resource_json ->> '$.date') AS \`vaccineDate\`
        FROM Patient AS p
        LEFT JOIN Immunization i ON (
            i.resource_json ->> '$.patient.reference' = CONCAT('Patient/', p.resource_id)`;

        // Only take immunizations made before the end of the year
        sql += ` AND DATE(i.resource_json ->> '$.date') <= DATE("${endDate}")`;

        // Only take HPV, MCV4P and Tdap immunizations
        sql += ` AND i.code IN('${vaccineCodes.join("', '")}')`;

        // Only take patients who would turn 13 years within the selected year
        sql += ` )
        WHERE
            DATE_ADD(p.resource_json ->> '$.birthDate', INTERVAL 13 YEAR) > Date("${startDate}") AND
            DATE_ADD(p.resource_json ->> '$.birthDate', INTERVAL 13 YEAR) < Date("${endDate}")`;

        sql += " ORDER BY DATE(i.resource_json ->> '$.date')";

        console.log(sql);

        const [rows] = await pool.query(sql);

        let patients     = {};
        let DENOMINATOR  = 0;
        let NUMERATORS   = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        rows.forEach(row => {
            let pt = patients[row.id];
            if (!pt) {
                pt = patients[row.id] = {
                    "62" : 0, // HPV, quadrivalent
                    "114": 0, // meningococcal MCV4P
                    "115": 0  // Tdap
                };
                DENOMINATOR += 1;
            }

            // Don't care for repeated immunizations
            if (pt.completed) return;

            if (row.vaccineCode) {
                pt[row.vaccineCode] = 1;
                if (pt["62"] && pt["114"] && pt["115"]) {
                    let month = moment(row.vaccineDate).month();
                    NUMERATORS[month] += 1;
                    pt.completed = 1;
                }
            }
        });

        let total = 0;
        NUMERATORS = NUMERATORS.map(cur => {
            total += cur;
            return total;
        });
        console.log(DENOMINATOR, NUMERATORS);

        const tasks = NUMERATORS.map((numerator, index) => DB.promise(
            "run",
            "INSERT OR REPLACE INTO measure_results_2 (" +
                "measure_id, date, numerator, denominator, org_id, ds_id" +
            ") VALUES (?, ?, ?, ?, ?, ?)",
            [
                "immunization_for_adolescents",
                `${year}-${index + 1 < 10 ? "0" + (index + 1) : index + 1 }-01`,
                numerator,
                DENOMINATOR,
                orgId,
                dsId
            ]
        ));

        await Promise.all(tasks);
    }

    static async getAll({ startDate, endDate, org, measure, ds })
    {

        startDate = "2016-01-01";
        endDate   = "2017-12-31";

        // ---------------------------------------------------------------------
        // Start Date
        // If none is specified, default to the start of the previous year.
        // In both cases the given date is shifted to the start of it's month.
        // ---------------------------------------------------------------------
        startDate = moment(startDate + "");
        if (!startDate.isValid()) {
            startDate = moment().startOf("year").subtract(1, "year");
        }
        startDate = startDate.startOf("month");


        // ---------------------------------------------------------------------
        // End Date
        // If none is specified, default to the end of the previous month.
        // In both cases the given date is shifted to the end of it's month.
        // ---------------------------------------------------------------------
        endDate = moment(endDate + "");
        if (!endDate.isValid()) {
            endDate = moment().subtract(1, "month");
        }
        endDate = endDate.endOf("month");


        // ---------------------------------------------------------------------
        // Synchronize.
        // Note that we don't wait for this to complete - it runs in the background!
        // ---------------------------------------------------------------------
        // MeasureResult.syncImmunizationsForAdolescents(startDate.year(), "bch", "bch_cerner");
        // MeasureResult.syncImmunizationsForAdolescents(endDate.year(), "bch", "bch_cerner");
        // MeasureResult.syncHypertension(startDate.year(), "bch", "bch_cerner");
        // MeasureResult.syncHypertension(endDate.year(), "bch", "bch_cerner");


        // ---------------------------------------------------------------------
        // Organizations
        // This is a list of organization IDs to be included in the response.
        // If no IDs are provided all organizations are selected. Otherwise,
        // only the passed IDs are selected.
        // ---------------------------------------------------------------------
        let orgSQL = "SELECT * FROM organizations";
        let orgParams = [];
        org = lib.makeArray(org).filter(Boolean);
        if (org.length) {
            orgSQL += ` WHERE id IN(${org.map(() => "?").join(", ")})`;
            orgParams = org;
        }
        const organizations = await DB.promise("all", orgSQL, orgParams);
        if (!organizations.length) {
            throw new Error("No organization(s) found");
        }

        // ---------------------------------------------------------------------
        // Measures
        // This is a list of measure IDs to be included in the response.
        // If no IDs are provided all measures are selected. Otherwise,
        // only the passed IDs are selected.
        // ---------------------------------------------------------------------
        let measureSQL = "SELECT * FROM measures WHERE enabled = 1";
        let measureParams = [];
        measure = lib.makeArray(measure).filter(Boolean);
        if (measure.length) {
            measureSQL += ` AND id IN(${measure.map(() => "?").join(", ")})`;
            measureParams = measure;
        }
        const measures = await DB.promise("all", measureSQL, measureParams);
        if (!measures.length) {
            throw new Error("No measure(s) found");
        }

        // ---------------------------------------------------------------------
        // DataSources
        // This is a list of dataSource IDs to be included in the response.
        // If no IDs are provided all the dataSources are selected. Otherwise,
        // only the passed IDs are selected.
        // ---------------------------------------------------------------------
        let dsSQL = "SELECT * FROM ds";
        let dsParams = [];
        ds = lib.makeArray(ds).filter(Boolean);
        if (ds.length) {
            dsSQL += ` WHERE id IN(${ds.map(() => "?").join(", ")})`;
            dsParams = ds;
        }
        const dataSources = await DB.promise("all", dsSQL, dsParams);
        if (!dataSources.length) {
            throw new Error("No dataSource(s) found");
        }

        ////////////////////////////////////////////////////////////////////////
        const out = {
            startDate,
            endDate,
            // results,
            organizations: {},
            measures
        };

        const results = await DB.promise(
            "all",

            `SELECT
                mr.id,
                mr.org_id,
                mr.date,
                mr.measure_id,
                SUM(mr.numerator) AS numerator,
                SUM(mr.denominator) AS denominator 
            FROM
                measure_results_2 AS mr
                JOIN measures AS m ON m.id = mr.measure_id
            WHERE
                m.enabled = 1
                AND mr.date >= ? AND mr.date <= ?
                
            GROUP BY mr.org_id, mr.measure_id, mr.date 
            ORDER BY mr.date, mr.org_id`,

            startDate.format("YYYY-MM-DD"),
            endDate.format("YYYY-MM-DD")
        );

        organizations.forEach(org => {
            const _org = {
                name: org.name,
                description: org.description,
                // dataSources,
                measures: []
            };

            measures.forEach(measure => {
                const data = {};

                // // For each month in the selected range
                // let prev = 0;
                // forEachMonth(startDate, endDate, date => {
                //     let idx = date.month();
                //     if (idx === 0) prev = 0;
                //     let pct = randomPercent(prev);
                //     data[date.format("YYYY-MM")] = pct;
                //     prev = pct;
                // });
                results.forEach(rec => {
                    if (rec.org_id === org.id && rec.measure_id === measure.id) {
                        const key = rec.date.replace(/-\d\d$/, "");
                        data[key] = {
                            numerator: rec.numerator,
                            denominator: rec.denominator,
                            pct: lib.roundToPrecision(rec.numerator / rec.denominator * 100, 2),
                            id: rec.id
                        };
                    }
                });

                _org.measures.push({
                    id  : measure.id,
                    name: measure.name,
                    data,
                    // results
                });
            });

            out.organizations[org.id] = _org;
        });

        return out;
    }

    /**
     * Returns the data needed to render the report page.
     */
    static getReport({ date, measure, org, ds })
    {
        if (!Array.isArray(ds)) ds = [ds];

        return DB.promise(
            "get",
            "SELECT " +
                // "mr.clinic_id        AS clinicID, " +
                "mr.date             AS measureDate, " +
                "m.name              AS measureName, " +
                "SUM(mr.numerator)   AS numeratorValue, " +
                "SUM(mr.denominator) AS denominatorValue, " +
                "m.description       AS measureDescription, " +
                "m.numerator         AS numeratorDescription, " +
                "m.denominator       AS denominatorDescription, " +
                "org.name            AS orgName, " +
                // "mr.numerator/mr.denominator * 100       AS value, " +
                "m.cohort_sql " +
            "FROM measure_results_2 AS mr " +
            "JOIN measures AS m ON mr.measure_id = m.id " +
            "JOIN organizations AS org ON mr.org_id = org.id " +
            // "JOIN clinic AS cl ON mr.clinic_id = cl.id " +
            "WHERE m.id = ? " +
            "AND \"date\" >= ? " +
            "AND \"date\" <= ? " +
            "AND mr.org_id = ? " +
            "AND ds_id IN(" + ds.map(() => "?").join(", ") + ")" +
            "GROUP BY mr.org_id, mr.measure_id, mr.date " +
            "ORDER BY mr.date, mr.org_id",
            measure,
            moment(date).startOf("month").format("YYYY-MM-DD"),
            moment(date).endOf("month").format("YYYY-MM-DD"),
            org,
            ...ds
        );
    }

    /**
     * Finds the SQL query associated with this measure. Then executes the query
     * and pipes the result stream to the given response stream.
     * The front-end uses this to render the report grid.
     * @param {Express.Response} res The HTTP response to pipe the result to
     */
    async getCohort(res) {
        const sql = (await DB.promise(
            "get",
            "SELECT m.cohort_sql AS sql " +
            "FROM measure_results AS mr " +
            "JOIN measures AS m ON mr.measure_id = m.id " +
            "WHERE mr.id=?",
            this.id
        )).sql;

        let source = client.createRowStream(sql, {});

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
    }
}

module.exports = MeasureResult;
