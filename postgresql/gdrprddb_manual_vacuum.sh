#!/bin/bash

# Source common variables
. /servers/config/gdr.env.sh

# Get script name and run tracker script
SCRIPT_NAME=`basename $0`; export SCRIPT_NAME
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME START

echo "START: `date`"

edb-psql -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} <<EOF

\timing
\echo "Following tables participate in manual vacuum as autovacuums are disabled on them".
select sysdate from dual;

vacuum verbose analyze epa2003.epaxml;
vacuum verbose analyze epa2003.epa_unit_noxt;
vacuum verbose analyze epa2003.epa_unit_soxt;
vacuum verbose analyze epa2003.t_epa_hourly_fueltype;
vacuum verbose analyze neta.tib_fpn;
vacuum verbose analyze neta.fpn_interm;
vacuum verbose analyze neta.neta_minute_partition_t;
vacuum verbose analyze ev_analysis.ev_hourly;
vacuum verbose analyze euro_analysis.entity_values;
vacuum verbose analyze euro_emissions.wsidata;   

select sysdate from dual;
\q      
EOF

echo "END: `date`"

# Run tracker script for end
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME END

