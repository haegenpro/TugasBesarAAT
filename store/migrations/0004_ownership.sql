-- Small coordinator control state. These tables must participate in the
-- coordinator's HA/durability policy; they contain no canonical dataset copy.
CREATE TABLE canonical_ownership (
    scope text COLLATE "C" PRIMARY KEY,
    source text NOT NULL CHECK (source IN ('BMKG','PVMBG','SATELIT','SENSOR_PANTAI')),
    epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    holder text,
    expires_at timestamptz,
    CHECK ((holder IS NULL AND expires_at IS NULL) OR
           (epoch > 0 AND holder IS NOT NULL AND holder <> '' AND expires_at IS NOT NULL))
);
INSERT INTO canonical_ownership(scope,source) VALUES
 ('canonical:BMKG','BMKG'), ('canonical:PVMBG','PVMBG'),
 ('canonical:SATELIT','SATELIT'), ('canonical:SENSOR_PANTAI','SENSOR_PANTAI');

-- Each protected transaction registers one check per scope. Its row is removed
-- by the deferred trigger, so successful commits do not accumulate receipts.
CREATE TABLE canonical_ownership_checks (
    transaction_id bigint NOT NULL CHECK (transaction_id = txid_current()),
    scope text COLLATE "C" NOT NULL REFERENCES canonical_ownership(scope),
    epoch bigint NOT NULL CHECK (epoch > 0),
    holder text NOT NULL CHECK (holder <> ''),
    PRIMARY KEY (transaction_id,scope)
);
CREATE FUNCTION canonical_check_ownership() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE authority public.canonical_ownership%ROWTYPE;
BEGIN
    SELECT * INTO authority FROM public.canonical_ownership
      WHERE scope=NEW.scope FOR SHARE;
    -- clock_timestamp(), unlike now(), advances during a transaction. Evaluate
    -- it after acquiring the authority row lock, including any lock wait.
    IF NOT FOUND OR authority.epoch <> NEW.epoch OR
       authority.holder IS DISTINCT FROM NEW.holder OR
       authority.expires_at IS NULL OR authority.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION USING ERRCODE='P0F01', MESSAGE='ownership expired before commit';
    END IF;
    DELETE FROM public.canonical_ownership_checks
      WHERE transaction_id=NEW.transaction_id AND scope=NEW.scope;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER canonical_ownership_at_commit
AFTER INSERT ON canonical_ownership_checks
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION canonical_check_ownership();
