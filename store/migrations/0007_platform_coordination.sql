-- Platform metadata only: no processor graph or frozen canonical rows here.
INSERT INTO canonical_ownership(scope,source) VALUES ('processor:SATELIT','SATELIT');

CREATE TABLE platform_audit_gate (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    epoch bigint NOT NULL DEFAULT 0 CHECK(epoch>=0),
    capture_id uuid,
    holder text,
    expires_at timestamptz,
    CHECK ((capture_id IS NULL AND holder IS NULL AND expires_at IS NULL) OR
           (capture_id IS NOT NULL AND holder IS NOT NULL AND expires_at IS NOT NULL))
);
INSERT INTO platform_audit_gate(singleton) VALUES(true);

CREATE TABLE platform_audit_captures (
    capture_id uuid PRIMARY KEY,
    epoch bigint NOT NULL,
    holder text NOT NULL,
    source text CHECK(source IN ('BMKG','PVMBG','SATELIT','SENSOR_PANTAI')),
    state text NOT NULL CHECK(state IN ('building','ready','aborted','expired')),
    views_pinned boolean NOT NULL DEFAULT false,
    topology text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    cut_at timestamptz,
    owner_expires_at timestamptz NOT NULL,
    copy_deadline timestamptz NOT NULL,
    idle_expires_at timestamptz,
    absolute_expires_at timestamptz NOT NULL,
    reason text,
    cleaned_at timestamptz
);
CREATE TABLE platform_capture_checks (
    transaction_id bigint NOT NULL CHECK(transaction_id=txid_current()),
    capture_id uuid NOT NULL REFERENCES platform_audit_captures(capture_id),
    epoch bigint NOT NULL,
    holder text NOT NULL,
    PRIMARY KEY(transaction_id,capture_id)
);
CREATE FUNCTION platform_check_capture() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE capture public.platform_audit_captures%ROWTYPE;
BEGIN
    SELECT * INTO capture FROM public.platform_audit_captures WHERE capture_id=NEW.capture_id FOR SHARE;
    IF NOT FOUND OR capture.epoch<>NEW.epoch OR capture.holder<>NEW.holder OR
       capture.state NOT IN ('building','ready') OR NOT capture.views_pinned OR
       capture.owner_expires_at<=clock_timestamp() OR capture.copy_deadline<=clock_timestamp() THEN
        RAISE EXCEPTION USING ERRCODE='P0F01',MESSAGE='capture authority expired before commit';
    END IF;
    DELETE FROM public.platform_capture_checks WHERE transaction_id=NEW.transaction_id AND capture_id=NEW.capture_id;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER platform_capture_at_commit AFTER INSERT ON platform_capture_checks
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION platform_check_capture();

CREATE TABLE platform_snapshot_read_checks (
    transaction_id bigint NOT NULL CHECK(transaction_id=txid_current()),
    capture_id uuid NOT NULL REFERENCES platform_audit_captures(capture_id),
    PRIMARY KEY(transaction_id,capture_id)
);
CREATE FUNCTION platform_check_snapshot_read() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE snapshot public.platform_audit_captures%ROWTYPE;
BEGIN
    SELECT * INTO snapshot FROM public.platform_audit_captures WHERE capture_id=NEW.capture_id FOR SHARE;
    IF NOT FOUND OR snapshot.state<>'ready' OR snapshot.idle_expires_at<=clock_timestamp() OR
       snapshot.absolute_expires_at<=clock_timestamp() THEN
        RAISE EXCEPTION USING ERRCODE='P0F01',MESSAGE='snapshot expired before read completion';
    END IF;
    DELETE FROM public.platform_snapshot_read_checks WHERE transaction_id=NEW.transaction_id AND capture_id=NEW.capture_id;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER platform_snapshot_read_at_commit AFTER INSERT ON platform_snapshot_read_checks
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION platform_check_snapshot_read();
