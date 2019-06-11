const { Router } = require("express");
const db         = require("./db");
const router     = exports.router = Router({ mergeParams: true });

router.get("/", (req, res) => {
    res.json({
        dataSources  : db.dataSources,
        measures     : db.measures,
        organizations: db.organizations,
        payers       : db.payers
    });
});

// const input = {

//     /**
//      * One or more organizations to include.
//      */
//     organizations: [],

//     /**
//      * The payer
//      */
//     payer: null,

//     /**
//      * One or more clinics to include. Set to `null` to include all clinics
//      */
//     clinics: null,

//     /**
//      * One or more data sources to include
//      */
//     dataSources: [],

//     /**
//      * The date starting from which to include results. This will be rounded to
//      * month precision. Defaults to the start of of the previous year.
//      */
//     startDate: null,

//     /**
//      * The end date included in the results. This will be rounded to
//      * month precision. Defaults to the start of of the current month.
//      */
//     endDate: null
// };

// function getMeasureResults(measure, filter = {})
// {
//     const {
//         organization,
//         payer,
//         clinic
//     } = filter;
// }