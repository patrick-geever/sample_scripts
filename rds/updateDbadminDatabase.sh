#!/bin/bash

# updateDbadminDatabase.sh - Patrick Geever - June 2019

echo "START: `date`"

# date string var
V_DATE=$(date "+%Y%m%d")
# V_TIMESTAMP=`date "+%C%y%m%dT%H%M"`

export PATH=$PATH:/opt/PostgreSQL/10/bin:/usr/local/bin
COMPANY_HOME=/u01/app/company
access_key=`cat ${COMPANY_HOME}/bin/updateDbadminDatabase.key.txt`
LOG_FILE=${COMPANY_HOME}/log/updateDbadminDatabase.log
LOG_HIST_FILE=${COMPANY_HOME}/log/updateDbadminDatabase.${V_DATE}.log
CLOUD_HEALTH_JSON_FILE=${COMPANY_HOME}/log/AwsRdsInstance.jq.pp.${V_DATE}.json
CLOUD_HEALTH_CSV_FILE=${COMPANY_HOME}/log/AwsRdsInstance.${V_DATE}.csv

export PGHOST=dbadmin-prod.cnqxj8dvxgxu.us-east-1.rds.amazonaws.com
export PGPORT=5432
export PGDATABASE=dbadmin
export PGUSER=dbadmin_app


# if anything fails set ERROR_FLAG=1 and report an error
ERROR_FLAG=0
function err_check { if [ $? -ne 0 ]; then ERROR_FLAG=1; fi }


curl -H "Authorization: Bearer ${access_key}" -H 'Accept: application/json'  "https://chapi.cloudhealthtech.com/api/search?name=AwsRdsInstance&include=account,region,instance_type,availability_zone,rds_subnet_group" | jq . > ${CLOUD_HEALTH_JSON_FILE}
err_check

# get values from json, pipe to "tr" translate double quotes to single quotes and pipe to sed to change empty hourly cost fields to 0.00. Blank field breaks insert/update
cat ${CLOUD_HEALTH_JSON_FILE} | jq -r ' .[] | [.account.name,  .account.owner_id,  .instance_id,  .create_date,  .region.name,  .engine,  .version,  .auto_update,  .is_multi_zone,  (.endpoint | split(":") | .[0]),  (.endpoint | split(":") | .[1]),  .username,  .flavor,  (.price_per_month | gsub("\\$"; "") | gsub(","; "") ),  (.hourly_cost | gsub("\\$"; "") | gsub(","; "") ),  .is_active  ] | @csv' | tr '"' "'" | sed "s/,'',/,'0.00',/" > ${CLOUD_HEALTH_CSV_FILE}
err_check

IFS='
'
for i in `cat ${CLOUD_HEALTH_CSV_FILE} `
do
    
    # split input line into variables
    IFS=, read Account_Name Account_Number Instance_Name Create_Date Region Engine Version Auto_Update Is_Multi_Zone Endpoint Port Master_User Db_Class PPM PPH Is_Active <<< "${i}"
    
    #pg echo "DB Instance: ${Instance_Name}"
    # echo "Account_Name = $Account_Name and Account_Number = $Account_Number and Instance_Name = $Instance_Name and Create_Date = $Create_Date"
    db_instance=`psql -q -t -c "select * from dbadmin.rds_instances where Account_Name = $Account_Name and Account_Number = $Account_Number and Instance_Name = $Instance_Name and Create_Date = $Create_Date;"`
    err_check
    
    
    if [ -z "${db_instance}" ]
    then
	# echo "No existing row for this instance"
	
	# Does account belong to safety
	v_account=`psql -q -t -c "select * from dbadmin.aws_accounts where Account_Name = $Account_Name and Account_Number = $Account_Number;"`
        err_check
	if (echo $v_account | grep 'safety' > /dev/null)
	then 
echo "DB Instance: ${Instance_Name}"
	    echo "New db instance found, insert row"
	    echo "${i}"
	    
            psql -c "insert into dbadmin.rds_instances ( dba_state, Account_Name, Account_Number, Instance_Name, Create_Date, Region, Engine, Version, Auto_Update, Is_Multi_Zone, Endpoint, Port, Master_User, Db_Class, PPM, PPH, Is_Active ) values  (  'current', $Account_Name, $Account_Number, $Instance_Name, $Create_Date, $Region, $Engine, $Version, $Auto_Update, $Is_Multi_Zone, $Endpoint, $Port, $Master_User, $Db_Class, $PPM, $PPH, $Is_Active); "
            err_check
	    
	    
	    #  Check for deleted db
	    #  select * from aws_instances where account_name, account_number, instance_id, and create_date < new.row.create_date;
	    #  if found update dba_state = 'deleted'
	    deleted_db_instance=`psql -q -t -c "select * from dbadmin.rds_instances where Account_Name = $Account_Name and Account_Number = $Account_Number and Instance_Name = $Instance_Name and Create_Date < $Create_Date ;"`
            err_check
	    echo "deleted_db_instance = ${deleted_db_instance}"
	    if [ -z "${deleted_db_instance}" ]
	    then
		echo "No deleted db instance in table"
	    else
		psql -c "update dbadmin.rds_instances set dba_state = 'deleted' where Account_Name = $Account_Name and Account_Number = $Account_Number and Instance_Name = $Instance_Name and Create_Date < $Create_Date ;"
		err_check
	    fi
	else
            :
            #echo "DB Instance: ${Instance_Name}"
	    #echo "Not Safety, Skipping"
	fi
	
    else
echo "DB Instance: ${Instance_Name}"
	echo "Check for update"
	# echo "${db_instance}"
	
	# query db, will have to format output to compare with jq output in csv format. Must set PGTZ=UTC to get right timestamp strings
	export PGTZ=UTC
	db_string=`psql  <<EOF
\COPY ( select  Account_Name, Account_Number, Instance_Name, to_char(create_date, 'YYYY-MM-DD HH24:MI:SS TZ'), Region, Engine, Version, Auto_Update::text, Is_Multi_Zone::text, Endpoint, Port, Master_User, Db_Class, PPM::text, PPH::text, Is_Active::text from dbadmin.rds_instances where Account_Name = $Account_Name and Account_Number = $Account_Number and Instance_Name = $Instance_Name and Create_Date = $Create_Date ) TO STDOUT WITH CSV;
\q
EOF`
	err_check
	
	# strip out quotes from jq string
	jq_string=`echo "${i}" | tr -d "\'"`
	
	#echo $db_string
	#echo $jq_string
	
	
	db_string_md5=`echo $db_string | md5sum`
	jq_string_md5=`echo $jq_string | md5sum`
	
	echo $db_string_md5
	echo $jq_string_md5
	
	if [ ${db_string_md5} =  ${jq_string_md5}  ]
	then
	    echo "No Changes, Continuing"
	else
	    echo "update with new values"
	    psql -c "update dbadmin.rds_instances set region = $Region, Engine = $Engine, version = $Version, auto_update = $Auto_Update, Is_Multi_Zone = $Is_Multi_Zone, Endpoint = $Endpoint, port = $Port, master_user = $Master_User, Db_Class = $Db_Class, ppm = $PPM, pph = $PPH, is_active = $Is_Active where Account_Name = $Account_Name and Account_Number = $Account_Number and Instance_Name = $Instance_Name and Create_Date = $Create_Date;"
	    err_check
	fi
    fi
done

# Check for deleted instances where no new instance is re-added
# select account_name, account_number, instance_name from dbadmin.rds_instances where dba_state = 'current'
# grep account_name, account_number, instance_name in csv file. If found continue, if missing mark row or rows as deleted
echo "Checking for deleted dbs"
for d in `psql -q --tuples-only -c "select '''' || account_name || ''','''  || account_number || ''',''' || instance_name || '''' from dbadmin.rds_instances where dba_state = 'current';" | sed 's/^ //' | sed '$d'`
do
# echo "${d}"
   # split input line into variables
    IFS=, read Account_Name Account_Number Instance_Name <<< "${d}"

   if ( grep "${d}" ${CLOUD_HEALTH_CSV_FILE} > /dev/null )
   then
       :
   else
       # mark row or rows as deleted
       echo ""${d}" is NOT in file for today, marking as deleted"
       psql -c "update dbadmin.rds_instances set dba_state = 'deleted' where Account_Name = $Account_Name and Account_Number = $Account_Number and Instance_Name = $Instance_Name;"
       err_check
   fi
done


if [ ${ERROR_FLAG} -eq 0 ]
then
   echo "SUCCESS - no errors"
else
   echo "ERROR: Check logfile"
fi

echo "END: `date`"

# find logs over month old and delete
find ${COMPANY_HOME}/log -type f -name 'updateDbadminDatabase.2*.log' -mtime +31 -exec rm -v {} \;
find ${COMPANY_HOME}/log -type f -name 'AwsRdsInstance.jq.pp.2*.json' -mtime +31 -exec rm -v {} \;
find ${COMPANY_HOME}/log -type f -name 'AwsRdsInstance.2*.csv' -mtime +31 -exec rm -v {} \;

cp -v $LOG_FILE $LOG_HIST_FILE
gzip -vf ${CLOUD_HEALTH_JSON_FILE}


if [ ${ERROR_FLAG} -eq 0 ]
then
   exit 0
else
   exit 1
fi
