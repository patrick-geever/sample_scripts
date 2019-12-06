#!/bin/bash

if [ ${#} -ne 1 ]
then
   echo "Usage: $0 RdsInstanceProperty"
   echo "Example:"
   echo "$0 DBInstanceStatus"
   echo "$0 StorageEncrypted"
   exit
fi

PROPERTY=$1
cred_profiles=`grep '^\[' ~/.aws/credentials | tr -d '[' | tr -d ']' | tr '\n' ' ' `

for PROFILE in `echo $cred_profiles`
do
echo "Profile = $PROFILE"

db_instances=`aws rds describe-db-instances --profile $PROFILE | grep '"DBInstanceIdentifier":' | cut -d ':' -f 2 | tr -d '"' | tr -d ' ' | tr '\n' ' ' `

   for DB in `echo $db_instances`
   do
      echo -n "DB_Instance: $DB"
      aws rds describe-db-instances --db-instance-identifier $DB --profile $PROFILE  | grep $PROPERTY
   done

echo
echo "############################################################################################"

done
