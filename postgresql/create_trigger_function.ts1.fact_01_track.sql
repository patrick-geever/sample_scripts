-- Function: ts1.fact_01_tracking_f()

-- DROP FUNCTION ts1.fact_01_tracking_f();

CREATE OR REPLACE FUNCTION ts1.fact_01_tracking_f()
  RETURNS trigger AS
$BODY$
DECLARE

v_status character(1);
v_userid varchar(50);

BEGIN

v_userid := current_user;

if tg_op = 'INSERT' 
then 
   v_status := 'I';
elseif tg_op = 'UPDATE' 
then 
   v_status := 'U';
elseif tg_op = 'DELETE' 
then 
   v_status := 'D';
end if;  

if v_status = 'I' or v_status = 'U'
then
  insert into ts1.fact_01_track  (
   id, rc_id, item_id, eff_date, value, audit_time, publish_time, status, job_queue_id, userid
    ) values (
    new.id, new.rc_id, new.item_id, new.eff_date, new.value, new.audit_time, new.publish_time, v_status, new.job_queue_id, v_userid
   );
elseif v_status = 'D'
then
  insert into ts1.fact_01_track  (
    id, rc_id, item_id, eff_date, value, audit_time, publish_time, status, job_queue_id, userid
   ) values (
    old.id, old.rc_id, old.item_id, old.eff_date, old.value, old.audit_time, old.publish_time, v_status, old.job_queue_id, v_userid
   );
end if;

  RETURN NEW;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION ts1.fact_01_tracking_f()
  OWNER TO gccqa;
GRANT EXECUTE ON FUNCTION ts1.fact_01_tracking_f() TO public;
GRANT EXECUTE ON FUNCTION ts1.fact_01_tracking_f() TO gccqa;
GRANT EXECUTE ON FUNCTION ts1.fact_01_tracking_f() TO role_write_schema_ts1;

