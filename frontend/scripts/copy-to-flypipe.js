// Once we build the js artifact containing all the frontend code we need to
// copy this into the Flypipe python code so that it can use the artifact.

var fs = require("fs");

fs.copyFile("dist/bundle.js", "../flypipe/js/bundle.js", (err) => {
    if (err) {
        console.log("ERROR copying bundle.js to Flypipe Python code");
        console.log(err);
    } else {
        console.log("Successfully copied bundle.js to Flypipe Python code");
    }
});
