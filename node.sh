#!/bin/bash

nodeps=$(ps -ef | grep 'node app.js')

pid="$(echo ${nodeps} | cut -d " " -f2)"
is_node="$(echo ${nodeps} | cut -d " " -f8)"

if [ $1 = "status" ]
then
    if [ $is_node = "node" ]
    then
        echo "node is running"
    else
        echo "node is NOT running"
    fi
elif [ $1 = "start" ]
then
    if [ $is_node = "node" ]
    then
        kill -9 $pid
        echo "node is restarting..."
    else
        echo "node is starting..."
    fi
    nohup node app.js > /dev/null 2>&1 &
elif [ $1 = "stop" ]
then
    if [ $is_node = "node" ]
    then
        kill -9 $pid
        echo "Successfully stopped node"
    else
        echo "node already stopped!"
    fi
else
    echo "Invalid Parameter!"
fi
