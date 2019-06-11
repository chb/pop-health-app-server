const { Router } = require("express");
const bodyParser = require("body-parser");
const Crypto     = require("crypto");
const lib        = require("./lib");
const db         = require("./db");
const router     = exports.router = Router({ mergeParams: true });

// The authentication middleware
exports.authenticate = (req, res, next) => {
    const { sid } = req.cookies;
    if (sid) {
        req.user = db.users.find(u => u.sid === sid);
        next();
    } else {
        next();
    }
};

// The login function
async function login({ username = "", password = "" }) {

    // Introduce artificial delay to protect against automated brute-force attacks
    await lib.resolveAfter(500);

    // Look up the user in DB
    const user = db.users.find(u => u.username === username);

    // No such username
    // Do NOT specify what is wrong in the error message!
    if (!user) {
        throw new Error("Invalid username or password");
    }

    // Wrong password
    // Do NOT specify what is wrong in the error message!
    if (user.password !== password) {
        throw new Error("Invalid username or password");
    }

    // Generate SID and update the user in DB
    const sid = Crypto.randomBytes(32).toString("hex");

    // Update user's lastLogin and sid properties
    user.sid = sid;
    user.lastLogin = new Date();

    // return the logged-in user
    return user;
}

// POST /login { username, password }
router.post("/login", bodyParser.urlencoded({ extended: true }), async (req, res) => {
    try {
        const user = await login(req.body);
        res.cookie("sid", user.sid, { httpOnly: true });
        res.json({
            username : user.username,
            lastLogin: user.lastLogin
        });
    } catch (ex) {
        console.error(ex);
        res.status(401).json({ error: ex.message });
    }
});

// GET /logout
router.get("/logout", async (req, res) => {

    // Introduce artificial delay to protect against automated brute-force attacks
    await lib.resolveAfter(500);

    if (req.user) {
        req.user.sid = null;
    }

    res.clearCookie("sid").json({ success: true });
});
