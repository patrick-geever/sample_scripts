-- Trigger: tr_client_txn_ack_partition on metrics.client_txn_ack

-- DROP TRIGGER tr_client_txn_ack_partition ON metrics.client_txn_ack;

CREATE TRIGGER tr_client_txn_ack_partition
  BEFORE INSERT
  ON metrics.client_txn_ack
  FOR EACH ROW
  EXECUTE PROCEDURE metrics.tr_f_client_txn_ack_partition('week');

