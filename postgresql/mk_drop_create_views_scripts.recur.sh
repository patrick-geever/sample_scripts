#!/bin/bash


if [ "${#}" -ne 5 -a "${#}" -ne 6 ]
then
echo "USAGE:"
echo "$0 HOSTNAME PORT DBNAME DBUSERID Schema.Tablename"
echo "or"
echo "$0 HOSTNAME PORT DBNAME DBUSERID Schema.Tablename PASSWORD"
exit;
fi

dbhost=$1
dbport=$2
dbname=$3
dbuserid=$4
tablename=$5

# will use .pgpass if exists, otherwise this is required
export PGPASSWORD=$6

tablename_schema=`echo $tablename | cut -d '.' -f1`
tablename_name=`echo $tablename | cut -d '.' -f2`

# test db connection
dbtest=`psql --quiet --tuples-only -h ${dbhost} -p ${dbport} -d ${dbname} -U ${dbuserid} <<EOF | tr -s ' ' | sed 's/^ //' | sed '$d'
select 1;
\q
EOF`

if [ "${dbtest}" != 1 ]
then
        echo "ERROR: DB Connection Failed"
        echo "Check Connection Parameters"
        exit
fi

dropfile=drop_views.${tablename}.${dbname}.sql
createfile=create_views.${tablename}.${dbname}.sql


create_views_func () {
psql --quiet --tuples-only -h ${dbhost} -p ${dbport} -d ${dbname} -U ${dbuserid} <<EOF | sed 's/^ //' | sed '$d' >> $createfile
select 'create view ' || schemaname || '.' || viewname || ' as ' || definition 
from pg_views where schemaname = '${v1_schema}' and viewname = '${v1_view}';

select 'ALTER TABLE ' || schemaname || '.' || viewname || ' OWNER TO ' || viewowner || ';' 
from pg_views where schemaname = '${v1_schema}' and viewname = '${v1_view}';
select 'GRANT ALL ON TABLE ' || schemaname || '.' || viewname || ' TO ' || viewowner || ';' 
from pg_views where schemaname = '${v1_schema}' and viewname = '${v1_view}';
select 'GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE ' || schemaname || '.' || viewname || ' TO role_write_schema_${v1_schema};' 
from pg_views where schemaname = '${v1_schema}' and viewname = '${v1_view}';
select 'GRANT SELECT ON TABLE ' || schemaname || '.' || viewname || ' TO role_read_schema_${v1_schema};' 
from pg_views where schemaname = '${v1_schema}' and viewname = '${v1_view}';
\q
EOF
}


get_views_func () {
local local_views="$1"
local v1=""
local v1_schema=""
local v1_view=""
local views2=""

for v1 in $local_views
do

echo "Checking:"
echo "$v1"

views2=`psql --quiet --tuples-only -h ${dbhost} -p ${dbport} -d ${dbname} -U ${dbuserid} <<EOF | sed 's/^ //' | sed '$d'
set search_path = public;

select views
     from (select distinct(r.ev_class::regclass) as views
            from pg_depend d join pg_rewrite r on r.oid = d.objid
           where refclassid = 'pg_class'::regclass
             and refobjid = '${v1}'::regclass
             and classid = 'pg_rewrite'::regclass
) as x
where views != '${v1}'::regclass
;
\q
EOF`



if [ -n "$views2" ]
then

	echo "Dependent Views:"
	echo "$views2"
	echo

        # create view
        v1_schema=`echo $v1 | cut -d '.' -f1`
        v1_view=`echo $v1 | cut -d '.' -f2`
	create_views_func

	echo -e "\n\n" >> $createfile

        # calling recursive function for dependent view
        get_views_func "$views2"

	# drops
	echo "drop view $v1;" >> $dropfile

else
	echo "no dependent views"
	echo "drop view $v1;"  >> $dropfile
	echo
	
	# create view
        v1_schema=`echo $v1 | cut -d '.' -f1`
        v1_view=`echo $v1 | cut -d '.' -f2`
	create_views_func

	echo -e "\n\n" >> $createfile

fi

done

}


table_or_view=`psql --quiet --tuples-only -h ${dbhost} -p ${dbport} -d ${dbname} -U ${dbuserid} <<EOF | sed 's/^ //' | sed '$d'
select c.relkind
from pg_class c
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
where c.relname = '${tablename_name}'
and n.nspname = '${tablename_schema}';
\q
EOF`


if [ -n "$table_or_view" ]
then

   if [ "$table_or_view" = "r" ]
   then
   echo "running table query"
   # for table
   views1=`psql --quiet --tuples-only -h ${dbhost} -p ${dbport} -d ${dbname} -U ${dbuserid} <<EOF | sed 's/^ //' | sed '$d'
   set search_path = public;

   select views
     from (select distinct(r.ev_class::regclass) as views
            from pg_depend d join pg_rewrite r on r.oid = d.objid
           where refclassid = 'pg_class'::regclass
             and refobjid = '${tablename}'::regclass
             and classid = 'pg_rewrite'::regclass
   ) as x
   ;
   \q
EOF`
   else
   echo "running view query"
   # for views
   views1=`psql --quiet --tuples-only -h ${dbhost} -p ${dbport} -d ${dbname} -U ${dbuserid} <<EOF | sed 's/^ //' | sed '$d'
   set search_path = public;

   select views
     from (select distinct(r.ev_class::regclass) as views
            from pg_depend d join pg_rewrite r on r.oid = d.objid
           where refclassid = 'pg_class'::regclass
             and refobjid = '${tablename}'::regclass
             and classid = 'pg_rewrite'::regclass
   ) as x
   where views != '${tablename}'::regclass
   ;
   \q
EOF`
   fi

else
        echo "Table or view does not exist"
	exit
fi


if [ -e ${dropfile} ]; then rm -v $dropfile; fi
if [ -e ${createfile} ]; then rm -v $createfile; fi

echo "begin;" >> $dropfile
echo "-- Recreate views for table ${tablename}" >> $createfile


echo "Top Level:"
echo "$views1"
echo

# call function
get_views_func "$views1"

echo "commit;" >> $dropfile

echo
echo "See files:"
echo  "$dropfile"
echo  "$createfile"
echo

