"""Validated operator tuning. Omitted keys preserve the deployed defaults."""
from copy import deepcopy


SQL_SERVICES = ('aggregator', 'auth-service', 'consumer-dashboard', 'consumer-notifier',
                'ingestion', 'wildfire-processor', 'coastal-processor', 'field-report-processor')
INGESTION_SERVICES = SQL_SERVICES[4:]
DEFAULTS = {
    'observability': {
        'trace_sample_ratio': 0.05, 'trace_queue_spans': 128, 'trace_batch_spans': 32,
        'trace_export_timeout_ms': 2000, 'ai_trace_export_timeout_ms': 1000,
        'prometheus_scrape_seconds': 15, 'prometheus_retention_hours': 6,
        'prometheus_retention_mib': 256, 'jaeger_max_traces': 1000,
        'loki_retention_hours': 24,
    },
    'database': {
        'services': {
            name: {'max_open': 6 if name == 'aggregator' else 4 if name in INGESTION_SERVICES else 8,
                   'statement_timeout_ms': 5000 if name in INGESTION_SERVICES else 1500,
                   'lock_timeout_ms': 2000 if name in INGESTION_SERVICES else 500}
            for name in SQL_SERVICES
        },
        'ingestion_idle_transaction_timeout_ms': 10000,
        'aggregator_field_read_connections': 3, 'registry_max_open': 4,
    },
    'admission': {
        'client_max_inflight': 64, 'client_per_client': 16, 'client_field_reserved': 16,
        'client_auth_max_inflight': 32, 'client_auth_field_reserved': 8,
        'auth_max_inflight': 32, 'auth_field_reserved': 8, 'auth_db_field_reserved': 3,
    },
    'sources': {
        'satellite_idle_timeout_ms': 20000, 'coastal_request_timeout_ms': 1200,
        'coastal_capture_timeout_ms': 8000, 'coastal_station_refresh_ms': 30000,
        'coastal_poll_interval_ms': 250, 'coastal_requests_per_second': 4,
        'coastal_breaker_failures': 3, 'coastal_breaker_cooldown_ms': 2000,
        'coastal_breaker_max_cooldown_ms': 15000,
    },
    'runtime': {
        'application_memory_mib': 192, 'aggregator_memory_mib': 384, 'mock_memory_mib': 96,
        'monitoring_memory_mib': 192, 'grafana_memory_mib': 128, 'go_processors': 2,
    },
    'ai': {
        'summary_calls_per_day': 45, 'chat_calls_per_day': 150,
        'refresh_interval_seconds': 1800, 'min_call_interval_seconds': 120,
        'llm_timeout_seconds': 60, 'chat_timeout_seconds': 60, 'chat_concurrency': 2,
        'max_tokens': 4096, 'reasoning_tokens': 1024,
    },
}


def _merge(default, override, path):
    if not isinstance(override, dict):
        raise ValueError(f'{path} must be an object')
    unknown = override.keys() - default.keys()
    if unknown:
        raise ValueError(f'unknown configuration key: {path}.{sorted(unknown)[0]}')
    result = deepcopy(default)
    for key, value in override.items():
        name = f'{path}.{key}'
        if isinstance(default[key], dict):
            result[key] = _merge(default[key], value, name)
        elif key == 'trace_sample_ratio':
            if type(value) not in (int, float) or not 0 <= value <= 1:
                raise ValueError(f'{name} must be a finite number between 0 and 1')
            result[key] = value
        else:
            # Bounds prevent overflow in SDK millisecond durations and counts.
            minimum = 0 if name == 'ai.chat_calls_per_day' else 1
            if type(value) is not int or not minimum <= value <= 2147483647:
                raise ValueError(f'{name} must be an integer between {minimum} and 2147483647')
            result[key] = value
    return result


def settings(cfg):
    result = {key: _merge(default, cfg.get(key, {}), key) for key, default in DEFAULTS.items()}
    obs, db, admission, sources = (result[key] for key in ('observability', 'database', 'admission', 'sources'))
    if obs['trace_batch_spans'] > obs['trace_queue_spans']:
        raise ValueError('trace_batch_spans must not exceed trace_queue_spans')
    for reserved, total in [('client_field_reserved', 'client_max_inflight'),
                            ('client_auth_field_reserved', 'client_auth_max_inflight'),
                            ('auth_field_reserved', 'auth_max_inflight')]:
        if admission[reserved] >= admission[total]:
            raise ValueError(f'admission.{reserved} must be below {total}')
    if admission['client_per_client'] > admission['client_max_inflight']:
        raise ValueError('client_per_client must not exceed client_max_inflight')
    if admission['auth_db_field_reserved'] >= db['services']['auth-service']['max_open']:
        raise ValueError('auth_db_field_reserved must be below the Auth database pool size')
    if sources['coastal_breaker_cooldown_ms'] > sources['coastal_breaker_max_cooldown_ms']:
        raise ValueError('coastal breaker cooldown must not exceed its maximum')
    for key, cap in [('application_memory_mib', 256), ('aggregator_memory_mib', 512),
                     ('mock_memory_mib', 128), ('monitoring_memory_mib', 256), ('grafana_memory_mib', 256)]:
        if result['runtime'][key] >= cap:
            raise ValueError(f'runtime.{key} must remain below the M2 container memory limit ({cap} MiB)')
    ai = result['ai']
    for key, low, high in [('summary_calls_per_day', 1, 10000), ('chat_calls_per_day', 0, 10000),
                           ('chat_concurrency', 1, 16), ('max_tokens', 2000, 16384), ('reasoning_tokens', 256, 4096)]:
        if not low <= ai[key] <= high:
            raise ValueError(f'ai.{key} must be between {low} and {high}')
    if ai['reasoning_tokens'] * 2 > ai['max_tokens']:
        raise ValueError('AI reasoning tokens must leave at least half the token budget for the answer')
    return result


def application_env(cfg, name):
    tuning = settings(cfg)
    obs, db, admission, sources = (tuning[key] for key in ('observability', 'database', 'admission', 'sources'))
    runtime = tuning['runtime']
    memory = runtime['aggregator_memory_mib'] if name == 'aggregator' else runtime['mock_memory_mib'] if name in ('bmkg-mock', 'pvmbg-mock') else runtime['application_memory_mib']
    env = {
        'GOMEMLIMIT': f'{memory}MiB', 'GOMAXPROCS': str(runtime['go_processors']),
        'OTEL_TRACES_SAMPLER_ARG': str(obs['trace_sample_ratio']),
        'OTEL_BSP_MAX_QUEUE_SIZE': str(obs['trace_queue_spans']),
        'OTEL_BSP_MAX_EXPORT_BATCH_SIZE': str(obs['trace_batch_spans']),
        'OTEL_BSP_EXPORT_TIMEOUT': str(obs['ai_trace_export_timeout_ms'] if name == 'situation-summarizer'
                                       else obs['trace_export_timeout_ms']),
    }
    if name in db['services']:
        sql = db['services'][name]
        env.update(DB_MAX_OPEN_CONNS=str(sql['max_open']),
                   DB_STATEMENT_TIMEOUT=f"{sql['statement_timeout_ms']}ms",
                   DB_LOCK_TIMEOUT=f"{sql['lock_timeout_ms']}ms")
    if name in INGESTION_SERVICES:
        env['DB_IDLE_TRANSACTION_TIMEOUT'] = f"{db['ingestion_idle_transaction_timeout_ms']}ms"
    if name == 'aggregator':
        env['AGGREGATOR_FIELD_DB_CONNS'] = str(db['aggregator_field_read_connections'])
    if name == 'schema-registry':
        return {'QUARKUS_DATASOURCE_JDBC_MAX_SIZE': str(db['registry_max_open'])}
    if name == 'sensor-pantai':
        return {}
    if name == 'client-api':
        env.update({wire: str(admission[key]) for wire, key in {
            'CLIENT_API_MAX_INFLIGHT': 'client_max_inflight', 'CLIENT_API_MAX_INFLIGHT_PER_CLIENT': 'client_per_client',
            'CLIENT_API_FIELD_RESERVED': 'client_field_reserved', 'CLIENT_API_AUTH_MAX_INFLIGHT': 'client_auth_max_inflight',
            'CLIENT_API_AUTH_FIELD_RESERVED': 'client_auth_field_reserved'}.items()})
    if name == 'auth-service':
        env.update({wire: str(admission[key]) for wire, key in {
            'AUTH_MAX_INFLIGHT': 'auth_max_inflight', 'AUTH_FIELD_RESERVED': 'auth_field_reserved',
            'AUTH_DB_FIELD_RESERVED': 'auth_db_field_reserved'}.items()})
    if name == 'ingestion':
        env.update({wire: str(sources[key]) for wire, key in {
            'SATELLITE_IDLE_TIMEOUT_MS': 'satellite_idle_timeout_ms',
            'PANTAI_REQUEST_TIMEOUT_MS': 'coastal_request_timeout_ms',
            'PANTAI_CAPTURE_TIMEOUT_MS': 'coastal_capture_timeout_ms',
            'PANTAI_STATION_REFRESH_MS': 'coastal_station_refresh_ms',
            'PANTAI_POLL_INTERVAL_MS': 'coastal_poll_interval_ms',
            'PANTAI_REQUESTS_PER_SECOND': 'coastal_requests_per_second',
            'PANTAI_BREAKER_FAILURES': 'coastal_breaker_failures',
            'PANTAI_BREAKER_COOLDOWN_MS': 'coastal_breaker_cooldown_ms',
            'PANTAI_BREAKER_MAX_COOLDOWN_MS': 'coastal_breaker_max_cooldown_ms'}.items()})
    if name == 'situation-summarizer':
        ai = tuning['ai']
        env.update({wire: str(ai[key]) for wire, key in {
            'SUMMARY_MAX_CALLS_PER_DAY': 'summary_calls_per_day', 'CHAT_MAX_CALLS_PER_DAY': 'chat_calls_per_day',
            'CHAT_MAX_CONCURRENT': 'chat_concurrency', 'LLM_MAX_TOKENS': 'max_tokens',
            'LLM_REASONING_TOKENS': 'reasoning_tokens'}.items()})
        env.update({wire: str(ai[key])+'s' for wire, key in {
            'SUMMARY_REFRESH_INTERVAL': 'refresh_interval_seconds', 'SUMMARY_MIN_CALL_INTERVAL': 'min_call_interval_seconds',
            'SUMMARY_LLM_TIMEOUT': 'llm_timeout_seconds', 'CHAT_TIMEOUT': 'chat_timeout_seconds'}.items()})
    return env
