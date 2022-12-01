`use strict`;
const args = process.argv.slice(2);
const config = require('./config.js')
const Promise = require('bluebird')
const mysql = require('mysql2/promise');
const { exec } = require("child_process");
const { writeFileSync, readFileSync } = require('fs');
const { dbName } = require('./config.js');

/*
Steps:
- Enable binlog and set binlog retention on master aurora
- both msater and slave should have gtid mode ON and gtid consistency enforce ON to avoid duplication of replication
- From the master aurora(one desired as master), create rds snapshot
- Restore the rds snapshot, this will allow you cpature the binlog position under the newly restore instance, sth like "Binlog position from crash recovery is mysql-bin-changelog.000002 3630015"
- Mysql dump from this snapshot rds (without binlog position info)
- Restore the dump to the slave mysql (azure or whatever place)
- at this stage, you have a slave mysql contain data in a specific binlog position (same as the snapshot event, however you will not able to get this position from the slave directly)
*/
/*
initmaster - create database and table in master db, this is for simular initial data in DB
generatemasterdata - insert data to master db, this is for testing ongoing replication
initslave - create dump from master db then restore to slave, this process should be done on a snapshot to avoid drifting mysql binlog position vs dump when DB have acitivity
startrepl - constantly export binlog from master and import to slave, it will write the latest position imported to latestpos.txt
*/
(async () => {
  switch (args[0]) {
    case 'initmaster':
      (async () => {
        let conn = await connectMasterDb();
        console.log(`recreating master database ${config.dbName}`)
        await conn.query(`DROP DATABASE IF EXISTS ${config.dbName};`)
        await conn.query(`CREATE DATABASE ${config.dbName};`)
        console.log(`generating data in master database ${config.dbName}`)
        await conn.query(`use ${config.dbName};`)
        await conn.query(
          `CREATE TABLE Persons (
        PersonId bigint NOT NULL AUTO_INCREMENT,
        PRIMARY KEY (PersonId)
      );`);
        await Promise.mapSeries(Array(10).fill(1), async () => {
          let sql = "INSERT INTO Persons() VALUES"
          for (let i = 0; i < 100; i++) {
            if (i === 0)
              sql += `()`
            else
              sql += `,()`
          }
          await conn.query(sql + ';')
        })
        let [[res]] = await conn.query("SELECT COUNT(*) as count FROM Persons;")
        console.log(`Persons table have ${res.count} rows`);
        await conn.end()
      })()
      break;
    case 'generatemasterdata':
      console.log(`generating data in master database ${config.dbName}`)
      await Promise.mapSeries(Array(1000).fill(1), async () => {
        const c = 100
        let sql = `INSERT INTO ${dbName}.Persons() VALUES`
        for (let i = 0; i < c; i++) {
          if (i === 0)
            sql += `()`
          else
            sql += `,()`
        }
        let conn = await connectMasterDb();
        await conn.query(sql + ';')
        let [[dbRes]] = await conn.query(`SELECT COUNT(*) as count FROM ${dbName}.Persons;`)
        await conn.end()
        console.log(`inserted ${c} to ${dbName}.Persons table, total ${dbRes.count} rows`)
      })
      break;
    case 'initslave':
      let conn = await connectMasterDb();
      let [[{ File, Position }]] = await conn.query(`SHOW MASTER STATUS;`)
      writeFileSync("latestpos.txt", Position + "")
      // this is not the right approach for database with live traffic as the mysql dump might excecuete with a different binlog Pos of the show master status, proper way is do this using a snapshot from RDS to "freeze" the position
      exec(__dirname + `\\mysqldump.exe --host ${config.master.host} -u ${config.master.user} --password=${config.master.password} --databases ${config.dbName} --single-transaction --set-gtid-purged=OFF > dump.sql`, (error, stdout, stderr) => {
        if (!error)
          console.log(`dump.sql created from master host, restoring to slave`)
        else
          console.log(stderr)
        exec(__dirname + `\\mysql.exe --host ${config.slave.host} -u ${config.slave.user} --password=${config.slave.password} < dump.sql`, async (error, stdout, stderr) => {
          if (!error)
            console.log(`dump.sql restored to slave`)
          else
            console.log(stderr)
          await conn.end()
        })
      });
      break;
    case 'startrepl':
      (async () => {
        let stopped = false
        while (!stopped) {
          // infinite loop
          await new Promise(async (res, rej) => {
            let conn = await connectMasterDb();
            let [[{ File, Position }]] = await conn.query(`SHOW MASTER STATUS;`)
            await conn.end();
            let lastPosition = readFileSync("latestpos.txt").toString()
            Position = Number(Position - 1)
            exec(__dirname + `\\mysqlbinlog.exe --read-from-remote-server -h ${config.master.host} -u ${config.master.user} --password=${config.master.password} ${File} --start-position=${lastPosition} --stop-position=${Position} > ./binlog/${lastPosition}-${Position}.binlog`, async (error, stdout, stderr) => {
              console.log(`exported binlog ${lastPosition}-${Position}.binlog`)
              exec(`gc ./binlog/${lastPosition}-${Position}.binlog | mysql -h ${config.slave.host} -u ${config.slave.user} --password=${config.slave.password}`, { 'shell': 'powershell.exe' }, async (error, stdout, stderr) => {
                console.log(`imported binlog ${lastPosition}-${Position}.binlog to slave`)
                writeFileSync("latestpos.txt", Position + "")
                let mConn = await connectMasterDb();
                let sConn = await connectSlaveDb();
                let [[mRes]] = await mConn.query(`select count(*) as count from ${config.dbName}.Persons;`)
                let [[sRes]] = await sConn.query(`select count(*) as count from ${config.dbName}.Persons;`)
                await mConn.end();
                await sConn.end();
                console.log(`Persons table have ${mRes.count} rows in master, ${sRes.count} rows in slave`);
                await sleep(1000)
                res()
              })
            })
          })
        }
      })()
      break;
    default:
      console.log('Sorry, that is not something I know how to do.');
  }
})()

async function connectMasterDb() {
  let conn = await mysql.createConnection({
    host: config.master.host,
    user: config.master.user,
    password: config.master.password
  });
  return conn;
}
async function connectSlaveDb() {
  let conn = await mysql.createConnection({
    host: config.master.host,
    user: config.master.user,
    password: config.master.password
  });
  return conn;
}
function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
} 
