#! /bin/bash

rm -rf bin/*.class
javac -cp ".;lib/postgresql-42.1.4.jar;" src/DBproject.java -d bin/


DBNAME=$1
PORT=$2
USER=$3

# Example: source ./run.sh flightDB 5432 user
#java -classpath "lib/*;bin/" DBproject $DBNAME $PORT $USER
java -classpath "lib/*:bin/" DBproject achen115_DB 5432 achen115

