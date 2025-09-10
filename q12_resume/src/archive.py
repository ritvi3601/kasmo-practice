import boto3
import config  # Make sure BUCKET_NAME is defined in config.py

# Initialize S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    region_name=config.AWS_REGION
)

def archive_resume(s3_key):
    """Move processed file from 'incoming/' to 'archive/' folder in S3"""
    copy_source = {"Bucket": config.BUCKET_NAME, "Key": s3_key}
    archive_key = s3_key.replace("incoming/", "archive/")
    
    # Copy to archive
    s3.copy_object(CopySource=copy_source, Bucket=config.BUCKET_NAME, Key=archive_key)
    
    # Delete original
    s3.delete_object(Bucket=config.BUCKET_NAME, Key=s3_key)
    
    print(f"Archived {s3_key} → {archive_key}")
