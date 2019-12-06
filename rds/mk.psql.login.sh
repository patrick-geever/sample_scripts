#!/bin/bash


if [ "${#}" -ne 1 ]
then
  echo
  echo "USAGE: $0 db-instance"
  echo 
  exit
fi


instance_name=$1

export PGHOST=dbadmin-prod.lsdjflskjsdx.us-east-1.rds.amazonaws.com
export PGPORT=5432
export PGDATABASE=dbadmin
export PGUSER=pgeever

echo

# out=`psql --quiet <<EOF
out=`psql dbadmin --quiet <<EOF
--suppress headings
\t
select 'Instance_Name: ' || Instance_Name || '
psql -h ' || endpoint || ' -p ' || port || ' -d DefaultDB -U ' || master_user from dbadmin.rds_instances where Instance_Name like '%${instance_name}%' and dba_state = 'current';
\q
EOF`

IFS='
'

for i in `echo "$out"`
do
  if (echo "$i" | grep -i redshift > /dev/null)
    then 
    echo "$i" | sed 's/DefaultDB/padb_harvest/'
  else
    echo "$i" | sed 's/DefaultDB/postgres/'
  fi
done

echo 
