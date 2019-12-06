-- Trigger: tr_fact_01_track_partition on ts1.fact_01_track

-- DROP TRIGGER tr_fact_01_track_partition ON ts1.fact_01_track;

CREATE TRIGGER tr_fact_01_track_partition
  BEFORE INSERT
  ON ts1.fact_01_track
  FOR EACH ROW
  EXECUTE PROCEDURE ts1.tr_f_fact_01_track_partition('week');

