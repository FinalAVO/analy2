const express = require('express');
const bodyParser = require('body-parser');
const {PythonShell} =require('python-shell');
const fs = require('fs')
// const shell = require('shelljs');
// const mysql = require('mysql');
const axios = require('axios');

const app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended : false}));
app.use(express.json());
app.use(express.urlencoded({extended : false}));

app.post('/analysis', function(req, res){
  var collection_name = req.body.collection_name;
  var user_id = req.body.user_id;
  var start_date = req.body.start_date;
  var end_date = req.body.end_date;

  // console.log(collection_name);

  let options_analy = {
    mode: 'text',
    pythonOptions: ['-u'],
    args: [collection_name, user_id, start_date, end_date]
  };

  analy_and_redis(options_analy, function(message){
    if (message == "Insert REDIS Done"){
      let options_insert = {
        mode: 'text',
        pythonOptions: ['-u'],
        args: [user_id]
      }

      insert_rds(options_insert, function(message){
        if (message == "Insert RDS Done"){
          res.send("Insert Done")
        } else {
          res.send("Insert Failed")
        }
      })
    } else {
      res.send("Analyze Failed")
    }
  })

});

// no longer needed
// app.post('/insert-rds', function(req,res){
//   var user_id = req.body.user_id;
//
//   let options_insert = {
//     mode: 'text',
//     pythonOptions: ['-u'],
//     args: [user_id]
//   }
//
//   insert_rds(options_insert, function(message){
//     if (message == "Insert RDS Done"){
//       res.send("Insert Done")
//     } else {
//       res.send("Insert Failed")
//     }
//   })
//
// });

let analy_and_redis = function(options, callback){
  PythonShell.run('./python_scripts/analysis.py', options, function (err, result){
          if (err) throw err;
          callback(result);
  });
}

let insert_rds = function(options, callback){
  PythonShell.run('./python_scripts/insert.py', options, function (err, result){
          if (err) throw err;
          callback(result);
  });
}


module.exports = app;
