/*
* CursusDB Observer for e2e test
* ******************************************************************
 */
import Observer from 'cursusdb-observer-node' // Observer for Node.js required
import {fs} from 'fs'

let ob = new Observer("sharedkey")
import fs from 'fs'
import util from 'util'

let log_file = fs.createWriteStream(__dirname + '/test.log', {flags : 'w'});
let log_stdout = process.stdout;

console.log = function(d) { //
    log_file.write(util.format(d) + '\n');
    log_stdout.write(util.format(d) + '\n');
};

if (ob.sharedKey !== undefined) {
    ob.Start() // Start listening

    ob.events.on('event', (data) => { // On observer events relay to websocket
        console.log(data)

    })
}
