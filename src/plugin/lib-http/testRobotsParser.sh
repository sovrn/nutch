#!/bin/bash

# figure out what the path separator char should be      
uname | grep -i cygwin > /dev/null
CYGWIN=$?
SEP=:
if [ $CYGWIN ]; then
   SEP=";"
fi

# Pick up jars for the classpath from the build                                                   
BUILD_DIR=../../../build/nutch*/build
LIB_DIR=../../../build/nutch*/lib

# Make sure lib-http and nutch jars are first
CPATH=`ls $BUILD_DIR/lib-http/lib-http.jar`$SEP`ls $BUILD_DIR/nutch*.jar`

# Add all the other jar files
for f in $LIB_DIR/*.jar
do
    CPATH="$CPATH$SEP$f"
done

# Go!
#echo $CPATH
java -classpath "$CPATH" org.apache.nutch.protocol.http.api.RobotRulesParser "$@"

