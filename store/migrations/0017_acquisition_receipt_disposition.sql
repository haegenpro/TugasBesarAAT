-- HTTP success is not application acceptance. Retire a receipt only after its
-- bytes are durably quarantined or accepted into the owning input store.
ALTER TABLE acquisition_dispatch DROP CONSTRAINT acquisition_dispatch_state_check;
ALTER TABLE acquisition_dispatch ADD CONSTRAINT acquisition_dispatch_state_check
 CHECK(state IN ('pending','complete','closed','abandoned','quarantined','consumed'));
