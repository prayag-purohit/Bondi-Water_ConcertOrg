# aws_utils.py
import os, boto3, io, logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")
LOG_KEY = f'Logs/app-session-at-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'

# Initialize S3 client
_s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
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


