#!/bin/bash

if [ "$#" -ne 2 ]
then
  echo
  echo "Usage: $0 password db_userid"
  echo
  exit
fi


password=$1
userid=$2

# select 'md5'||md5(?Some_Great_Passw0rd?||'<user_id>');
# select 'md5'||md5('x'||'kdunn');


md5string=`echo -n ${password}${userid} | md5sum | sed -e 's/  -$//' `

echo "create user ${userid} with encrypted password 'md5${md5string}';"
echo "alter user ${userid} with encrypted password 'md5${md5string}';"


echo
echo "VALID UNTIL 'infinity';"
