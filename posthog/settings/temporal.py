import os

from posthog.settings.utils import get_list

TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
# NOTE: TEMPORAL_TASK_QUEUE refactored to TEMPORAL_BATCH_EXPORTS_TASK_QUEUE
TEMPORAL_BATCH_EXPORTS_TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "no-sandbox-python-django")
TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "127.0.0.1")
TEMPORAL_PORT = os.getenv("TEMPORAL_PORT", "7233")
TEMPORAL_CLIENT_ROOT_CA = os.getenv("TEMPORAL_CLIENT_ROOT_CA", None)
TEMPORAL_CLIENT_CERT = os.getenv("TEMPORAL_CLIENT_CERT", None)
TEMPORAL_CLIENT_KEY = os.getenv("TEMPORAL_CLIENT_KEY", None)
TEMPORAL_WORKFLOW_MAX_ATTEMPTS = os.getenv("TEMPORAL_WORKFLOW_MAX_ATTEMPTS", "3")


BATCH_EXPORT_S3_UPLOAD_CHUNK_SIZE_BYTES = 1024 * 1024 * 50  # 50MB
BATCH_EXPORT_SNOWFLAKE_UPLOAD_CHUNK_SIZE_BYTES = 1024 * 1024 * 100  # 100MB
BATCH_EXPORT_POSTGRES_UPLOAD_CHUNK_SIZE_BYTES = 1024 * 1024 * 50  # 50MB
BATCH_EXPORT_BIGQUERY_UPLOAD_CHUNK_SIZE_BYTES = 1024 * 1024 * 100  # 100MB


UNCONSTRAINED_TIMESTAMP_TEAM_IDS = get_list(os.getenv("UNCONSTRAINED_TIMESTAMP_TEAM_IDS", ""))
