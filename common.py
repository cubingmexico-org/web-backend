import logging
import os
from functools import wraps

import psycopg2
from flask import abort, request
from google.cloud import secretmanager

EXCLUDED_EVENTS = ["333ft", "333mbo", "magic", "mmagic"]
SINGLE_EVENTS = ["333fm", "333bf", "333mbf", "444bf", "555bf"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def get_secret(secret_id, project_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "cubing-mexico")

# For local development, allow DB_URL to be set via environment variable.
# Otherwise, fetch from GCP Secret Manager.
DB_URL = os.environ.get("DB_URL")
if not DB_URL:
    log.info("DB_URL not found in environment, fetching from Secret Manager")
    DB_URL = get_secret("db_url", GCP_PROJECT_ID)
else:
    log.info("Using DB_URL from environment variable")


CRON_SECRET = os.environ.get("CRON_SECRET")
if not CRON_SECRET:
    log.info("CRON_SECRET not found in environment, fetching from Secret Manager")
    CRON_SECRET = get_secret("cron-secret", GCP_PROJECT_ID)
else:
    log.info("Using CRON_SECRET from environment variable")


def get_connection():
    return psycopg2.connect(DB_URL)


def require_cron_auth(f):
    """Decorator to restrict endpoints to authorized cron jobs only."""

    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get("Authorization")

        if not auth_header:
            log.warning("Missing Authorization header for %s", request.path)
            abort(403, description="Access forbidden: Missing authorization")

        try:
            scheme, token = auth_header.split()
            if scheme.lower() != "bearer":
                raise ValueError("Invalid scheme")
        except ValueError:
            log.warning("Invalid Authorization header format for %s", request.path)
            abort(403, description="Access forbidden: Invalid authorization format")

        if token != CRON_SECRET:
            log.warning("Invalid cron token for %s", request.path)
            abort(403, description="Access forbidden: Invalid credentials")

        return f(*args, **kwargs)

    return decorated_function
