import os

from dotenv import load_dotenv


load_dotenv()


AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")

S3_BUCKET = os.getenv("S3_BUCKET")

S3_BRONZE_PREFIX = os.getenv(
    "S3_BRONZE_PREFIX",
    "bronze/ecommerce-events",
)

S3_CHECKPOINT_PREFIX = os.getenv(
    "S3_CHECKPOINT_PREFIX",
    "checkpoints/ecommerce-events",
)


if not S3_BUCKET:
    raise ValueError(
        "S3_BUCKET is not configured in the .env file."
    )


S3_BRONZE_PATH = (
    f"s3a://{S3_BUCKET}/{S3_BRONZE_PREFIX}"
)

S3_CHECKPOINT_PATH = (
    f"s3a://{S3_BUCKET}/{S3_CHECKPOINT_PREFIX}"
)