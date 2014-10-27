// test that a rollback of an op more than 1800 secs newer than the new master causes fatal shutdown
if (false) {  // SERVER-15695
// set up a set and grab things for later
var name = "rollback_too_new";
var replTest = new ReplSetTest({name: name, nodes: 3});
var nodes = replTest.nodeList();
var conns = replTest.startSet();
replTest.initiate({"_id": name,
                   "members": [
                       { "_id": 0, "host": nodes[0], priority: 3 },
                       { "_id": 1, "host": nodes[1] },
                       { "_id": 2, "host": nodes[2], arbiterOnly: true}]
                  });
var a_conn = conns[0];
var b_conn = conns[1];
var AID = replTest.getNodeId(a_conn);
var BID = replTest.getNodeId(b_conn);

// get master and do an initial write
var master = replTest.getMaster();
assert(master === conns[0], "conns[0] assumed to be master");
assert(a_conn.host === master.host, "a_conn assumed to be master");
var options = {writeConcern: {w: 2, wtimeout: 60000}, upsert: true};
assert.writeOK(a_conn.getDB(name).foo.insert({x: 1}, options));

// shut down master
replTest.stop(AID);

// add an entry with ts more than 1800s newer which should cause fassert when rolled back
master = replTest.getMaster();
assert(b_conn.host === master.host, "b_conn assumed to be master");
options = {writeConcern: {w: 1, wtimeout: 60000}, upsert: true};
// insert to change minvalid so that we will need rollback
assert.writeOK(b_conn.getDB(name).foo.insert({x: 2}, options));
// new entry from the distant future
var oplog_entry = b_conn.getDB("local").oplog.rs.find().sort({$natural: -1})[0];
oplog_entry["ts"] = new Timestamp(oplog_entry["ts"].getTime() + 200000, 1)
assert.writeOK(b_conn.getDB("local").oplog.rs.insert(oplog_entry));

// shut down B and bring back the original master
replTest.stop(BID);
replTest.restart(AID);
master = replTest.getMaster();
assert(a_conn.host === master.host, "a_conn assumed to be master");

// do a write so that B will have to roll back
assert.writeOK(a_conn.getDB(name).foo.insert({x: 2}, options));

// restart B, which should attempt to rollback but then fassert.
clearRawMongoProgramOutput();
replTest.restart(BID);
assert.soon(function() {
    return rawMongoProgramOutput().match(
        "rollback error: not willing to roll back more than 30 minutes of data");
}, "B failed to fassert");

replTest.stopSet();
}
