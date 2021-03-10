#! /bin/bash
DBNAME=$1
PORT=$2
USER=$3

# Example: source ./run.sh flightDB 5432 user
#java -classpath "lib/*;bin/" DBproject $DBNAME $PORT $USER
java -classpath "lib/*;bin/" DBproject myDB 5432 alexm

