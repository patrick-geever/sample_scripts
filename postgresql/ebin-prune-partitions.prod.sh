#!/bin/bash

# Patrick Geever - June/July 2019

set -o pipefail
ERROR_FLAG=0

ENV=prod
local_logfile=ebin-prune-partitions.${ENV}.log
echo "DUMP STARTED: `date`" >> ${local_logfile}

PGHOST=eb01-postgres-gc-prod.slkdfjslkdjf.us-gov-west-1.rds.amazonaws.com
PGPORT=5432
PGDATABASE=eb01
PGUSER=partition_manager
export PGPASSWORD=${PROD_PGPASSWORD}
AWS_S3_BUCKET=data.us-gov-west-1.ebinserv.company.com
target_part_tables="alcatraz.import_job_aftermath delorean.audit_log"

# get date string for today
RETENTION_TIME=180

echo "TZ = $TZ"                                    
echo "PGHOST = $PGHOST"     
echo "PGPORT = $PGPORT"     
echo "PGDATABASE = $PGDATABASE"
echo "PGUSER = $PGUSER"        

# get target date string
#for gnu date: target_datestring=`date +%Y%m%d  --date="$RETENTION_TIME days ago"`
# for busybox date command:
target_datestring=`date  -I "+%Y%m%d" -d "@$(($(date +%s) - 86400 * ${RETENTION_TIME}))"`


function set_error_flag_function()
{
    RETURN_CODE=$?
    if [ ${RETURN_CODE} -ne 0 ]
    then
	ERROR_FLAG=1
    fi
}

function check_error_function
{
    if [ "${ERROR_FLAG}" -eq 0 ]
    then
	:
    else
	echo "ERROR: ${ENV}: ${1}: bailing out"
	echo "DUMP ENDED: ERROR: ${1}: `date`" >> ${local_logfile}
	exit $RETURN_CODE
    fi
}

echo "target_datestring = $target_datestring"
echo "target_datestring = $target_datestring" >> ${local_logfile}
echo "#########"


# loop to see if there are any tables to be dumped for target date
for tt in `echo "${target_part_tables}"`
do
    
    # split input line into variables
    IFS=. read v_schema v_tablename <<< "${tt}"
    
    # find partition tables that are older than retention date
    partitions_to_drop=`psql --tuples-only -h ${PGHOST} -p ${PGPORT} -d ${PGDATABASE} -U ${PGUSER} <<EOF | sed 's/^ //' | sed '$d'
select schemaname || '.' || tablename from pg_tables where tablename like '${v_tablename}_%' and tablename <= '${v_tablename}_${target_datestring}' and schemaname = '${v_schema}' order by tablename;
\q
EOF`
    set_error_flag_function
    check_error_function "psql partitions_to_drop error"
    
    partitions_to_drop_check=`echo "${partitions_to_drop_check}${partitions_to_drop}"`
    
done
unset partitions_to_drop

# test for empty list
if [ -z "${partitions_to_drop_check}" ]
then
    echo "no partitions to dump. Exiting"
    echo "RETURN_CODE = $RETURN_CODE"
    echo "DUMP ENDED: `date`" >> ${local_logfile}
    echo "`basename $0`: ${ENV}: Success"
    exit 0
fi


# loop for multiple tables to dump
for t in `echo "${target_part_tables}"`
do
    
    # split input line into variables
    IFS=. read v_schema v_tablename <<< "${t}"
    
    # find partition tables that are older than retention date
    partitions_to_drop=`psql --tuples-only -h ${PGHOST} -p ${PGPORT} -d ${PGDATABASE} -U ${PGUSER} <<EOF | sed 's/^ //' | sed '$d'
select schemaname || '.' || tablename from pg_tables where tablename like '${v_tablename}_%' and tablename <= '${v_tablename}_${target_datestring}' and schemaname = '${v_schema}' order by tablename;
\q
EOF`
    set_error_flag_function
    check_error_function "psql partitions_to_drop error"
    
    
    echo "Partitions to dump and drop:"
    for i in `echo ${partitions_to_drop}`
    do
	echo $i
    done
    echo "#########"
    
    
    # dump each table to text file in container with \copy to stdout in pipe delimited format
    for part_table in `echo ${partitions_to_drop}`
    do
	
	echo ${part_table} >> ${local_logfile}
	
	# run pg_dump to dump file
	pipedump_filename=${PGDATABASE}.${ENV}.${part_table}.txt
	pgdump_filename=${PGDATABASE}.${ENV}.${part_table}.dump
	psql -h ${PGHOST} -p ${PGPORT} -d ${PGDATABASE} -U ${PGUSER} -c "\copy ${part_table} to stdout WITH DELIMITER '|' NULL '';" > ${pipedump_filename}
	set_error_flag_function
	check_error_function "dump_partition_to_file error"
	
	
	# run pg_dump again to md5sum to get hash
	psql_direct_dump_hash=`psql -h ${PGHOST} -p ${PGPORT} -d ${PGDATABASE} -U ${PGUSER} -c "\copy ${part_table} to stdout WITH DELIMITER '|' NULL '';" | md5sum | tr -s '' | cut -d ' ' -f 1`
	set_error_flag_function
	check_error_function "pg_dump to md5sum to get hash error"
	
	# get hash of dumped pipe delimited file
	dump_file_hash=`md5sum ${pipedump_filename} | tr -s '' | cut -d ' ' -f 1`
	set_error_flag_function
	check_error_function "md5sum of dumpfile to get hash error"
	
	echo "table dump hash: ${psql_direct_dump_hash}"
	echo "dump file hash:  ${dump_file_hash}"
	
	# test for match
	if [ "${psql_direct_dump_hash}" ==  "${dump_file_hash}" ]
	then
	    echo "md5sum hashs match, continuing with ${part_table}"
	else
	    echo "Table: ${part_table}"
	    echo "Dumpfile: ${pipedump_filename}"
	    ERROR_FLAG=1
	    check_error_function "md5sum hash mismatch for psql pipe delimited dumps"
	fi
	
	
	# get row count on table to check against dump file
	table_row_count=`psql -h ${PGHOST} -p ${PGPORT} -d ${PGDATABASE} -U ${PGUSER} --tuples-only -c "select count(*) from ${part_table};" | tr -d ' ' | sed '$d' `
	set_error_flag_function
	check_error_function "get table row count error"
	
	# get row count from dumpfile
	pipedump_filename_row_count=`wc -l ${pipedump_filename} | tr -s '' | cut -d ' ' -f 1 `
	set_error_flag_function
	check_error_function "dumpfile row count error"
	
	echo "table rows:    ${table_row_count}"
	echo "dumpfile rows: ${pipedump_filename_row_count}"
	
	if [ ${table_row_count} -eq ${pipedump_filename_row_count} ]
	then
	    echo "row counts match, continuing with ${part_table}"
	else
	    echo "Table: ${part_table}"
	    echo "Dumpfile: ${pipedump_filename}"
	    ERROR_FLAG=1
	    check_error_function "Table and dumpfile row count mismatch"
	fi
	
	
	gzip -fv ${pipedump_filename}
	set_error_flag_function
	check_error_function "gzip dumpfile error"
	
	
	# get hash of compressed pipe delimited file 
	compressed_dump_file_hash=`md5sum ${pipedump_filename}.gz | tr -s '' | cut -d ' ' -f 1`
	set_error_flag_function
	check_error_function "md5sum of compressed dumpfile to get hash error"
	
	
	echo "Send to S3: aws s3 cp --quiet ${pipedump_filename}.gz s3://${AWS_S3_BUCKET}/${ENV}/archive//${pipedump_filename}.gz --sse"
	
	aws s3 cp --quiet ${pipedump_filename}.gz s3://${AWS_S3_BUCKET}/${ENV}/archive/${pipedump_filename}.gz --sse
	set_error_flag_function
	check_error_function "copy pipedump_file to s3 error"
	
	
	# test for file at s3 to be sure it has arrived ok at s3
	# run md5sum on ${pipedump_filename} 
	# run aws s3 cp bucket/${pipedump_filename} - | md5sum
	# test that sums match to be sure file arrived at s3 ok
	# if yes, drop local file and drop partition
	# if no, exit
	
	
	# get md5sums to compare
	echo "Copy back from S3: s3_md5sum=aws s3 cp s3://${AWS_S3_BUCKET}/${ENV}/archive/${pipedump_filename}.gz"' - | md5sum | cut -d ' ' -f 1 `'
	
	s3_md5sum=`aws s3 cp s3://${AWS_S3_BUCKET}/${ENV}/archive/${pipedump_filename}.gz - | md5sum | cut -d ' ' -f 1 `
	set_error_flag_function
	check_error_function "get md5sum hash from dumpfile copied back from s3 error"
	
	echo "s3_md5sum:                 ${s3_md5sum}"
	echo "compressed_dump_file_hash: ${compressed_dump_file_hash}"
	
	
	if [ "${s3_md5sum}" == "${compressed_dump_file_hash}" ]
	then
	    echo "dumpfiles match, continuing with pgdump of ${part_table}"
	else
	    ERROR_FLAG=1
	    RETURN_CODE=1
	    check_error_function "s3 dumpfile and local dumpfile mismatch, bailing out."
	fi
	
	
	rm -v ${pipedump_filename}.gz
	
	echo "#########"
	
	
	# add pg_dump of table
	# run dump of table through gzip to fs
	pg_dump -h ${PGHOST} -p ${PGPORT} -d ${PGDATABASE} -U ${PGUSER} --table=${part_table} | gzip -c > ${pgdump_filename}.gz
	set_error_flag_function
	check_error_function "regular pgdump to gzip error"
	
	# send to s3
	aws s3 cp --quiet ${pgdump_filename}.gz s3://${AWS_S3_BUCKET}/${ENV}/archive/pgdump/${pgdump_filename}.gz --sse
	set_error_flag_function
	check_error_function "copy pgdump_file to s3 error"
	
	# get checksum of local pgdump file
	pgdump_compressed_dump_file_hash=`md5sum ${pgdump_filename}.gz | tr -s '' | cut -d ' ' -f 1`
	set_error_flag_function
	check_error_function "md5sum of pgdump compressed dumpfile to get hash error"
	
	# get checksum of s3 file
	pgdump_s3_md5sum=`aws s3 cp s3://${AWS_S3_BUCKET}/${ENV}/archive/pgdump/${pgdump_filename}.gz - | md5sum | cut -d ' ' -f 1 `
	set_error_flag_function
	check_error_function "get md5sum hash from pgdump_file copied back from s3 error"
	
	# compare pgdump checksums
	if [ "${pgdump_s3_md5sum}" == "${pgdump_compressed_dump_file_hash}" ]
	then
	    echo "pgdump_files match, dropping partition ${part_table}"
	    # drop code
	    echo "dropping partition ${part_table}"
	    psql -h ${PGHOST} -p ${PGPORT} -d ${PGDATABASE} -U ${PGUSER} -c "drop table ${part_table};"
	    set_error_flag_function
	    check_error_function "Drop partition error"
	else
	    ERROR_FLAG=1
	    RETURN_CODE=1
	    check_error_function "s3 pgdump_file and local pgdump_file mismatch, bailing out."
	fi
	############################################
	
	rm -v ${pgdump_filename}.gz
	
	echo "#########"
	
	
	
    done # inside for loop end
    
done # outside for loop end

echo "DUMP ENDED: `date`" >> ${local_logfile}

echo "RETURN_CODE = $RETURN_CODE"

if [ ${ERROR_FLAG} -eq 0 ]
then
    echo "`basename $0`: ${ENV}: Success"
    exit 0
else 
    echo "ERROR:  ${ENV}: ERROR_FLAG = $ERROR_FLAG"
    exit 1
fi

