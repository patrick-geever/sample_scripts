-- Table: metrics.hvdf_msg_pv

-- DROP TABLE metrics.hvdf_msg_pv;

CREATE TABLE metrics.hvdf_msg_pv
(
  iso_composite_key character varying(100) NOT NULL,
  batch_id bigint NOT NULL,
  insert_timestamp timestamp with time zone NOT NULL,
  inserted_by character varying(25),
  CONSTRAINT hvdf_msg_pv_pkey PRIMARY KEY (iso_composite_key, batch_id),
  CONSTRAINT hvdf_msg_pv_batch_primary_value_fkey FOREIGN KEY (batch_id)
      REFERENCES metrics.hvdf_batch_pv (batch_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE metrics.hvdf_msg_pv
  OWNER TO metrics;
GRANT ALL ON TABLE metrics.hvdf_msg_pv TO metrics;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE metrics.hvdf_msg_pv TO role_write_schema_metrics;
GRANT SELECT ON TABLE metrics.hvdf_msg_pv TO role_read_schema_metrics;

-- Trigger: tr_hvdf_msg_pv on metrics.hvdf_msg_pv

-- DROP TRIGGER tr_hvdf_msg_pv ON metrics.hvdf_msg_pv;

CREATE TRIGGER tr_hvdf_msg_pv
  BEFORE INSERT
  ON metrics.hvdf_msg_pv
  FOR EACH ROW
  EXECUTE PROCEDURE metrics.indexed_partition_multiplexer_hvdf_msg_pv_by_week();

