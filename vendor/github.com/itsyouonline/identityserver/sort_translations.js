'use strict';
const fs = require('fs');
function sortObject(o) {
    return Object.keys(o).sort().reduce((r, k) => (r[k] = o[k], r), {});
}

let sourceDir = 'siteservice/website/assets/i18n';
fs.readdir(sourceDir, function (err, files) {
    if (err) {
        console.error("Could not list the directory.", err);
        process.exit(1);
    }
    files.forEach(function (file) {
        let path = `${sourceDir}/${file}`;
        fs.readFile(path, 'utf8', function (err, data) {
            if (err) {
                return console.log(err);
            }
            let translations = JSON.stringify(sortObject(JSON.parse(data)), null, 4);
            fs.writeFile(path, translations, function (err) {
                if (err) {
                    return console.error(err);
                }
            });
        });
    });
});