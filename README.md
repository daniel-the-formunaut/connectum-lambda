# Connectum S3 Archive Lambda

Simple AWS Lambda helper for archiving old objects from an S3 bucket into `Archiv/` and optionally reclassifying archived objects to a cheaper storage class.

## What it does

- Reads `S3_BUCKET` and `S3_ARCHIVE` from environment variables.
- Moves files older than `ARCHIVE_AFTER_DAYS` from the bucket root into `Archiv/<original-key>`.
- Copies archived files with `StorageClass` set by `ARCHIVE_STORAGE_CLASS`.
- Supports a second mode to reclassify all existing objects already under `Archiv/`.

## Files

- `code/lambda_function.py` — Lambda handler and CLI-compatible local runner.
- `.env` — local test environment values.

## Local test

1. Set local variables in `.env`:
   ```ini
   S3_BUCKET=formunauts-dataflow-connectum
   S3_ARCHIVE=Archiv/
   ARCHIVE_AFTER_DAYS=60
   ARCHIVE_STORAGE_CLASS=GLACIER_IR
   ```

2. Run default archive mode:
   ```powershell
   python .\code\lambda_function.py
   ```

3. Run reclassify mode to change objects under `Archiv/` to `GLACIER_IR`:
   ```powershell
   python -c "from code.lambda_function import lambda_handler; print(lambda_handler({'action':'reclassify_archive'}, None))"
   ```

## Lambda deployment

1. Package the file from repository root:
   ```powershell
   Set-Location .\code
   Compress-Archive -Path lambda_function.py -DestinationPath ..\lambda_package.zip
   ```

2. Create or update the Lambda function with handler:
   - `lambda_function.lambda_handler`

3. Configure Lambda env vars:
   - `S3_BUCKET`
   - `S3_ARCHIVE`
   - `ARCHIVE_AFTER_DAYS`
   - `ARCHIVE_STORAGE_CLASS`
   - optional `DATA_TEAM_ADMIN_ROLE_ARN`

4. Invoke the default archive path with no payload.

5. Invoke reclassify mode using payload:
   ```bash
   aws lambda invoke --function-name archive-old-s3-files --payload '{"action":"reclassify_archive"}' response.json
   ```

## Notes

- `.env` is only used for local testing.
- On Lambda, set the same variables in function configuration.
- `ARCHIVE_STORAGE_CLASS` default is `GLACIER_IR`.
- Lambda must have S3 permissions: `ListBucket`, `GetObject`, `PutObject`, `DeleteObject`.
