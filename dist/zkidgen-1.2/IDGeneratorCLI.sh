#!/bin/sh

VERSION="1.2"

if [ -f ./zkidgen-${VERSION}.jar ]; then
	# dist environment
	JAR_FOUND="y"
	JAR_DIR="."
else
	# svn root environment
	JAR_DIR="lib"
fi

if [ "y" = "$JAR_FOUND" ]; then 
	# prod/dist environment
	CLASSPATH="${JAR_DIR}/zkidgen-${VERSION}.jar"
	LOGCONF="${JAR_DIR}/log4j.properties"
else
	# svn root environment
	CLASSPATH="classes"
	LOGCONF="conf/log4j/log4j.properties"
fi

CLASSPATH="${CLASSPATH}:${JAR_DIR}/slf4j-api-1.5.6.jar"
CLASSPATH="${CLASSPATH}:${JAR_DIR}/slf4j-log4j12-1.5.6.jar"
CLASSPATH="${CLASSPATH}:${JAR_DIR}/log4j-1.2.15.jar"
CLASSPATH="${CLASSPATH}:${JAR_DIR}/zookeeper-3.3.0-bin.jar"

java \
	-cp $CLASSPATH \
	-Dlog4j.configuration=file:${LOGCONF} \
	com.demdex.idgen.IDGeneratorCLI \
	$*
