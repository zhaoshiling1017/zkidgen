#!/bin/bash

SERVER_LIST="$1"
if [ -z "$SERVER_LIST" ]; then
	echo -e "Usage:\ttest.sh <comma-separated-list-of-zookeeper-servers>"
	exit 1
fi

bin/IDGeneratorCLI.sh create "$SERVER_LIST" /test2 1-100000000000
# created

bin/IDGeneratorCLI.sh read "$SERVER_LIST" /test2
# Read idSet: /test2:1-100000000000

bin/IDGeneratorCLI.sh take "$SERVER_LIST" /test2 10000
# Took idSet: /test2:1-10000

bin/IDGeneratorCLI.sh read "$SERVER_LIST" /test2
# Read idSet: /test2:10001-100000000000

bin/IDGeneratorCLI.sh push "$SERVER_LIST" /test2 9001-10000
# pushed

bin/IDGeneratorCLI.sh read "$SERVER_LIST" /test2
# Read idSet: /test2:9001-10000,10001-100000000000
