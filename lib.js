
function resolveAfter(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
}

function makeArray(x) {
    if (Array.isArray(x)) {
        return x;
    }
    return [x];
}

module.exports = {
    resolveAfter,
    makeArray
};
