import datetime
import logging
import os
from pathlib import Path
from typing import Dict, Iterator, Optional

import boto3
from botocore.exceptions import ClientError

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DEFAULT_ARCHIVE_PREFIX = "Archiv/"
DEFAULT_ARCHIVE_DAYS = 60
DEFAULT_ARCHIVE_STORAGE_CLASS = "GLACIER_IR"
ALLOWED_STORAGE_CLASSES = {
    "STANDARD",
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "GLACIER_IR",
    "GLACIER",
    "DEEP_ARCHIVE",
}
RESTORABLE_STORAGE_CLASSES = {"GLACIER", "DEEP_ARCHIVE"}
RESTORE_DAYS = 1
RESTORE_TIER = "Standard"


def load_dotenv_from_root() -> None:
    """Load .env variables from repository root when local environment variables are missing."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def get_env_variable(name: str, default: Optional[str] = None, required: bool = True) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Environment variable '{name}' is required but was not set.")
    return value or ""


def _extract_role_name_from_arn(arn: str) -> Optional[str]:
    if ":assumed-role/" in arn:
        return arn.split(":assumed-role/", 1)[1].split("/", 1)[0]
    if ":role/" in arn and arn.startswith("arn:aws:iam::"):
        return arn.rsplit("/", 1)[1]
    return None


def _current_aws_role_name() -> Optional[str]:
    try:
        sts = boto3.client("sts")
        arn = sts.get_caller_identity()["Arn"]
        return _extract_role_name_from_arn(arn)
    except Exception as exc:
        LOGGER.warning("Unable to determine current AWS role name: %s", exc)
        return None


def get_aws_session() -> boto3.Session:
    role_arn = os.environ.get("DATA_TEAM_ADMIN_ROLE_ARN")
    if not role_arn:
        LOGGER.info("No DATA_TEAM_ADMIN_ROLE_ARN found; using default AWS credentials.")
        return boto3.Session()

    if role_arn.startswith("arn:aws:sts:"):
        raise RuntimeError(
            "DATA_TEAM_ADMIN_ROLE_ARN must be an IAM role ARN like "
            "arn:aws:iam::<account-id>:role/DataTeamAdmin, not an STS assumed-role ARN."
        )

    target_role_name = _extract_role_name_from_arn(role_arn)
    current_role_name = _current_aws_role_name()
    if current_role_name and target_role_name and current_role_name == target_role_name:
        LOGGER.info(
            "Current credentials are already using role '%s'; skipping AssumeRole.",
            target_role_name,
        )
        return boto3.Session()

    sts = boto3.client("sts")
    LOGGER.info("Assuming DataTeamAdmin role %s", role_arn)
    try:
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="S3ArchiveSession",
            DurationSeconds=3600,
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Unable to assume role {role_arn}: {exc.response.get('Error', {}).get('Message', str(exc))}"
        ) from exc

    credentials = response["Credentials"]
    return boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


def normalize_archive_prefix(prefix: str) -> str:
    if not prefix:
        prefix = DEFAULT_ARCHIVE_PREFIX
    prefix = prefix.replace("\\", "/")
    if prefix.startswith("/"):
        prefix = prefix.lstrip("/")
    if not prefix.endswith("/"):
        prefix += "/"
    return prefix


def validate_storage_class(storage_class: str) -> str:
    storage_class = storage_class.strip().upper()
    if storage_class not in ALLOWED_STORAGE_CLASSES:
        raise RuntimeError(
            f"Invalid ARCHIVE_STORAGE_CLASS '{storage_class}'. "
            f"Supported values: {', '.join(sorted(ALLOWED_STORAGE_CLASSES))}."
        )
    return storage_class


def object_restore_needed(storage_class: str, restore_header: str) -> bool:
    if storage_class not in RESTORABLE_STORAGE_CLASSES:
        return False
    if not restore_header:
        return True
    return "ongoing-request=\"true\"" in restore_header.lower()


def initiate_restore_object(s3_client, bucket: str, key: str) -> None:
    try:
        s3_client.restore_object(
            Bucket=bucket,
            Key=key,
            RestoreRequest={
                "Days": RESTORE_DAYS,
                "GlacierJobParameters": {"Tier": RESTORE_TIER},
            },
        )
        LOGGER.info("Restore request started for '%s'", key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code == "RestoreAlreadyInProgress":
            LOGGER.info("Restore already in progress for '%s'", key)
            return
        raise


def list_eligible_objects(
    s3_client, bucket: str, archive_prefix: str, older_than_days: int
) -> Iterator[Dict]:
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=older_than_days)
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.startswith(archive_prefix):
                continue
            if key.endswith("/"):  # skip folder placeholder keys
                continue
            last_modified = obj["LastModified"]
            if last_modified < cutoff:
                yield obj


def archive_object(
    s3_client, bucket: str, key: str, archive_prefix: str, storage_class: str
) -> None:
    target_key = f"{archive_prefix}{key}"
    LOGGER.info(
        "Archiving '%s' to '%s' using storage class %s",
        key,
        target_key,
        storage_class,
    )
    copy_source = {"Bucket": bucket, "Key": key}
    try:
        s3_client.copy_object(
            Bucket=bucket,
            Key=target_key,
            CopySource=copy_source,
            StorageClass=storage_class,
            MetadataDirective="COPY",
            TaggingDirective="COPY",
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
        LOGGER.info("Successfully archived '%s'", key)
    except ClientError as exc:
        LOGGER.error("Failed to archive '%s': %s", key, exc)
        raise


def list_archive_objects(s3_client, bucket: str, archive_prefix: str) -> Iterator[Dict]:
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=archive_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            yield obj


def reclassify_archive_objects(
    s3_client, bucket: str, archive_prefix: str, target_storage_class: str
) -> tuple[int, int]:
    total_reclassified = 0
    total_pending_restores = 0
    for obj in list_archive_objects(s3_client, bucket, archive_prefix):
        key = obj["Key"]
        head = s3_client.head_object(Bucket=bucket, Key=key)
        current_storage_class = head.get("StorageClass", "STANDARD").upper()
        restore_header = head.get("Restore", "")

        if object_restore_needed(current_storage_class, restore_header):
            initiate_restore_object(s3_client, bucket, key)
            total_pending_restores += 1
            continue

        if current_storage_class == target_storage_class:
            LOGGER.info(
                "Skipping '%s'; already in storage class %s", key, target_storage_class
            )
            continue

        LOGGER.info(
            "Changing storage class for '%s' from %s to %s",
            key,
            current_storage_class,
            target_storage_class,
        )
        copy_source = {"Bucket": bucket, "Key": key}
        try:
            s3_client.copy_object(
                Bucket=bucket,
                Key=key,
                CopySource=copy_source,
                StorageClass=target_storage_class,
                MetadataDirective="COPY",
                TaggingDirective="COPY",
            )
            total_reclassified += 1
        except ClientError as exc:
            LOGGER.error(
                "Failed to change storage class for '%s': %s", key, exc
            )
            raise

    LOGGER.info(
        "Reclassified %d archive object(s) to %s",
        total_reclassified,
        target_storage_class,
    )
    if total_pending_restores:
        LOGGER.info(
            "Initiated restore for %d archived object(s); re-run after restore completes.",
            total_pending_restores,
        )
    return total_reclassified, total_pending_restores


def lambda_handler(event, context):
    load_dotenv_from_root()
    bucket = get_env_variable("S3_BUCKET")
    archive_prefix = normalize_archive_prefix(get_env_variable("S3_ARCHIVE", DEFAULT_ARCHIVE_PREFIX, required=False))
    older_than_days = int(get_env_variable("ARCHIVE_AFTER_DAYS", str(DEFAULT_ARCHIVE_DAYS), required=False))

    session = get_aws_session()
    s3_client = session.client("s3")
    archive_storage_class = validate_storage_class(
        get_env_variable(
            "ARCHIVE_STORAGE_CLASS",
            DEFAULT_ARCHIVE_STORAGE_CLASS,
            required=False,
        )
    )
    action = (event or {}).get("action", "archive_old")

    if action == "reclassify_archive":
        total_reclassified, total_pending_restores = reclassify_archive_objects(
            s3_client,
            bucket,
            archive_prefix,
            archive_storage_class,
        )
        message = (
            f"Reclassified {total_reclassified} archive object(s) in bucket '{bucket}' "
            f"to storage class {archive_storage_class}."
        )
        if total_pending_restores:
            message += f" Restore initiated for {total_pending_restores} archived object(s); re-run after completion."
        LOGGER.info(message)
        return {
            "status": "reclassified",
            "bucket": bucket,
            "archive_prefix": archive_prefix,
            "storage_class": archive_storage_class,
            "objects_reclassified": total_reclassified,
            "objects_pending_restore": total_pending_restores,
        }

    total_archived = 0
    for obj in list_eligible_objects(s3_client, bucket, archive_prefix, older_than_days):
        archive_object(
            s3_client,
            bucket,
            obj["Key"],
            archive_prefix,
            archive_storage_class,
        )
        total_archived += 1

    message = (
        f"Archived {total_archived} object(s) from bucket '{bucket}' into '{archive_prefix}' "
        f"using storage class {archive_storage_class}. Older than {older_than_days} days."
    )
    LOGGER.info(message)
    return {
        "status": "completed",
        "bucket": bucket,
        "archive_prefix": archive_prefix,
        "older_than_days": older_than_days,
        "objects_archived": total_archived,
    }


if __name__ == "__main__":
    try:
        result = lambda_handler({}, None)
        print(result)
    except Exception as exc:
        LOGGER.exception("Local execution failed: %s", exc)
        raise
