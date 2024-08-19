#!/usr/bin/env bash
# Description how to use STILTS to execute a query
# Example of use:
# ./do_query_stilts.sh user outputfile.csv "SELECT TOP 10 * FROM jplus.TileImage"
url=https://archive.cefca.es/catalogues/vo/tap/jplus-dr3
user=$1
outputfile=$2
query=$3
unset pass
read -s -p Password: pass
java -jar -Dstar.basicauth.user=$user \
         -Dstar.basicauth.password=$pass \
         stilts.jar tapquery \
         tapurl=$url adql="$query" \
         ofmt=csv sync=false \
         out="$outputfile"