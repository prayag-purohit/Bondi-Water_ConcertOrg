# aws_utils.py
import os, boto3, io, logging
from datetime import datetime
import streamlit as st
from dotenv import load_dotenv
from pathlib import Path

# Load local .env if available
load_dotenv()

def secrets_file_exists():
    """Check if secrets.toml exists locally (to avoid Streamlit error)."""
    return Path('.streamlit/secrets.toml').is_file() or Path.home().joinpath('.streamlit/secrets.toml').is_file()

def get_secret(key):
    """Unified secrets loader for Streamlit Cloud or local .env."""
    if secrets_file_exists():
        try:
            value = st.secrets[key]
            #logger.info(f"Loaded {key} from Streamlit secrets.")
            return value
        except KeyError as e:
            print(f"KeyError: {e}")
            #logger.warning(f"{key} not found in secrets.toml.")
    else:
        #logger.info(f"Running locally. Fetching {key} from .env")
        return os.getenv(key)

# Load credentials and config
AWS_ACCESS_KEY_ID = get_secret("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = get_secret("AWS_SECRET_ACCESS_KEY")
AWS_REGION = get_secret("AWS_REGION")
BUCKET_NAME = get_secret("BUCKET_NAME")
#LOG_LOC = get_secret("LOG_LOC") or "Logs/"

LOG_KEY = f'Logs/app-session-at-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'

# Initialize S3 client
_s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# Initialize logger and log stream
_log_stream = io.StringIO()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),        # Console
        logging.StreamHandler(_log_stream)  # In-memory stream for S3 uploads
    ]
)
_logger = logging.getLogger(__name__)

def upload_log_to_s3(logger, log_stream):
    """
    needs logger, and log stream you can run the function in the end
    example_usage: upload_log_to_s3(logger, log_stream)
    """
    try:
        s3, BUCKET_NAME = get_s3_client_and_bucket_name()
        # Retrieve log content from the in-memory stream
        log_contents = log_stream.getvalue()
        # Upload log content to S3
        s3.put_object(Bucket=BUCKET_NAME, Key=LOG_KEY, Body=log_contents)
        logger.info("Log successfully uploaded to S3.")
    except Exception as e:
        logger.error(f"Failed to upload log to S3: {e}")

def get_s3_client_and_bucket_name():
    """Return the initialized S3 client."""
    return _s3_client, BUCKET_NAME

def get_logger_and_log_stream():
    """Return the initialized logger."""
    return _logger, _log_stream


