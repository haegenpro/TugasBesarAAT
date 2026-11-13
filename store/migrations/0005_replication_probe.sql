-- Administrator-installed on every physical PostgreSQL group. Bootstrap also
-- uses this file on workers; CREATE IF NOT EXISTS / OR REPLACE is intentional.
CREATE TABLE IF NOT EXISTS public.platform_storage_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    formation text NOT NULL CHECK (formation <> ''),
    group_id integer NOT NULL CHECK (group_id >= 0)
);
REVOKE ALL ON public.platform_storage_identity FROM PUBLIC;

-- Expose only the replication evidence needed by the application, rather than
-- granting pg_monitor or access to superuser control functions to its role.
CREATE OR REPLACE FUNCTION public.platform_replication_status() RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    recovering boolean := pg_is_in_recovery();
    inserted pg_lsn;
    flushed pg_lsn;
BEGIN
    IF NOT recovering THEN
        inserted := pg_current_wal_insert_lsn();
        flushed := pg_current_wal_flush_lsn();
    END IF;
    RETURN jsonb_build_object(
        'formation', (SELECT formation FROM public.platform_storage_identity WHERE singleton),
        'group_id', (SELECT group_id FROM public.platform_storage_identity WHERE singleton),
        'system_id', (SELECT system_identifier::text FROM pg_control_system()),
        'member', current_setting('cluster_name'),
        'primary', NOT recovering,
        'started', extract(epoch FROM pg_postmaster_start_time())::text,
        'timeline', CASE WHEN NOT recovering THEN left(pg_walfile_name(inserted),8) END,
        'insert_lsn', inserted::text,
        'flush_lsn', flushed::text,
        'fsync', current_setting('fsync'),
        'full_page_writes', current_setting('full_page_writes'),
        'synchronous_commit', current_setting('synchronous_commit'),
        'standby_names', current_setting('synchronous_standby_names'),
        'replicas', coalesce((SELECT jsonb_agg(jsonb_build_object(
            'member',application_name,'state',state,'sync_state',sync_state,
            'flush_lsn',flush_lsn::text)) FROM pg_stat_replication),'[]'::jsonb)
    );
END;
$$;
REVOKE ALL ON FUNCTION public.platform_replication_status() FROM PUBLIC;
