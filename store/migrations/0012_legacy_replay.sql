ALTER TABLE failed_ingestions DROP CONSTRAINT failed_ingestions_status_check;
ALTER TABLE failed_ingestions ADD CONSTRAINT failed_ingestions_status_check CHECK (status IN ('retrying','parked','replaying','recovered'));
ALTER TABLE failed_ingestions ADD COLUMN replay_adapter text, ADD COLUMN recovered_at timestamptz;
CREATE INDEX failed_ingestions_replaying ON failed_ingestions(source,received_at,message_id) WHERE status='replaying';
