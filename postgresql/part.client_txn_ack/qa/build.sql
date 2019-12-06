



\c metricsqa metrics
\i create_function.metrics.need_indexed_part_table_client_txn_ack.sql
\i create_trigger_function.metrics.tr_f_client_txn_ack_partition.sql

select mk_grants_to_schema_roles_by_owner('metrics');

\i create_trigger.metrics.tr_client_txn_ack_partition.sql


\c metricsqa postgres
select mk_grants_to_sympeer_role_by_schema_owner('metrics');


Add pg_hba.conf record for prod app servers
# App server
host    metricsqa     sympeer         192.168.38.169/32      md5    # peerrep, SymmetricDS App server
host    metricsqa     metrics         192.168.38.186/32      md5    # pvmllou113, pdbloomapp01
host    metricsqa     metrics         192.168.38.187/32      md5    # pvmllou115, pdbloomapp02
host    metricsqa     metrics         192.168.38.53/32       md5    # pvmllou99, pdcommon01
host    metricsqa     appl_metrics    192.168.38.53/32       md5    # pvmllou99, pdcommon01
host    metricsqa     metrics         192.168.39.8/32        md5    # hal.company.com
host    metricsqa     genview         192.168.38.218/32      md5    # pdnapwint

host    metricsqa     metrics         172.24.1.67/32         md5    # Mike Frohme Desktop


RESTART sympeer after migration.


Add old partition table pruning scripts


