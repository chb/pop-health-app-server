const DB = {
    "results": [
        // {
        //     "measure_id" : "",
        //     "org_id"     : "",
        //     "payer_id"   : "",
        //     "clinic_id"  : "",
        //     "dataset_id" : "",
        //     "month"      : "",
        //     "year"       : "",
        //     "numerator"  : 0,
        //     "denominator": 0
        // }
    ]
};

/**
 * Finds a single record from the Presto backend
 * @param {Object} options Filter params
 * @param {String} options.org_id
 * @param {String} options.clinic_id
 * @param {String} options.dataset_id
 * @param {String} options.payer_id
 * @param {String} options.measure_id
 * @param {String} options.year
 * @param {String} options.month
 */
function getResult(options)
{
    // Find record by org_id, payer_id, clinic_id, dataset_id, year and month
    let rec = DB.results.find(rec => (
        rec.org_id     === options.org_id     &&
        rec.payer_id   === options.payer_id   &&
        rec.measure_id === options.measure_id &&
        rec.clinic_id  === options.dataset_id &&
        rec.year       === options.year       &&
        rec.month      === options.month
    ));

    if (!rec) {
        const a = Math.round(Math.random() * 100);
        const b = Math.round(Math.random() * 100);

        rec = {
            org_id     : options.org_id,
            payer_id   : options.payer_id,
            clinic_id  : options.dataset_id,
            measure_id : options.measure_id,
            year       : options.year,
            month      : options.month,
            numerator  : Math.min(a, b),
            denominator: Math.max(a, b)
        };

        DB.results.push(rec);
    }

    return { numerator: rec.numerator, denominator: rec.denominator };
}

module.exports = {
    getResult
};
