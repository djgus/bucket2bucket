import boto3
import requests
import time
import sys
from tqdm import tqdm

# Variables
presigned_url = "presigned-url"
dest_bucket = "dest-bucket"
dest_key = "path/to/file"
chunk_size = 100 * 1024 * 1024 # 100 MB
prof_name = "dest-profile"
max_retries = 5
retry_delay = 3 # seconds

# Configure destination session with your profile
s3_client = boto3.Session(profile_name=prof_name).client('s3')

# Get file size from presigned URL
def get_file_size():
    response = requests.head(presigned_url)
    file_size = int(response.headers.get('content-length', 0))
    
    if file_size <= 0:
        with requests.get(presigned_url, headers={'Range': 'bytes=0-1'}, stream=True) as range_response:
            if 'Content-Range' in range_response.headers:
                content_range = range_response.headers['Content-Range']
                file_size = int(content_range.split('/')[-1])
    
    return file_size

# Check for existing upload or start a new one
uploads = s3_client.list_multipart_uploads(Bucket=dest_bucket).get('Uploads', [])
mpu = next((u for u in uploads if u['Key'] == dest_key), None)

# Get upload ID and track parts
if mpu:
    print(f"Resuming previous upload for {dest_key}")
    upload_id = mpu['UploadId']
    
    # Get all parts with pagination
    parts = []
    for page in s3_client.get_paginator('list_parts').paginate(Bucket=dest_bucket, Key=dest_key, UploadId=upload_id):
        parts.extend(page.get('Parts', []))
    
    uploaded_parts = {p['PartNumber']: p['ETag'] for p in parts}
    part_number = max(uploaded_parts.keys()) + 1 if uploaded_parts else 1
    bytes_completed = chunk_size * (part_number - 1)  # More accurate calculation
else:
    print(f"Starting new upload for {dest_key}")
    upload_id = s3_client.create_multipart_upload(Bucket=dest_bucket, Key=dest_key)['UploadId']
    uploaded_parts = {}
    part_number = 1
    bytes_completed = 0

# Get file size before starting download
file_size = get_file_size()
if file_size <= 0:
    print("Error: Could not determine file size")
    sys.exit(1)

# Set up Range header if resuming download
headers = {}
if bytes_completed > 0:
    headers['Range'] = f'bytes={bytes_completed}-'
    print(f"Resuming download from byte position {bytes_completed}")

# Initialize parts list for final completion
parts = [{'PartNumber': pn, 'ETag': etag} for pn, etag in uploaded_parts.items()]

# Set up progress bar
progress = tqdm(
    total=file_size, 
    initial=bytes_completed, 
    unit='B', 
    unit_scale=True,
    bar_format='{percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
)

try:
    # Start download with proper headers
    with requests.get(presigned_url, stream=True, headers=headers) as response:
        # Update file size if content-range is available
        if 'content-range' in response.headers:
            content_range = response.headers['content-range']
            total_size = int(content_range.split('/')[1])
            if total_size != file_size:
                file_size = total_size
                progress.total = file_size

        # Process the file in chunks
        for chunk in response.iter_content(chunk_size=chunk_size):
            if not chunk:
                continue
            
            # Skip already uploaded parts
            if part_number in uploaded_parts:
                progress.update(len(chunk))
                part_number += 1
                continue
                
            # Upload with retry logic
            for retry_count in range(max_retries):
                try:
                    part = s3_client.upload_part(
                        Body=chunk,
                        Bucket=dest_bucket,
                        Key=dest_key,
                        PartNumber=part_number,
                        UploadId=upload_id
                    )
                    
                    parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                    progress.update(len(chunk))
                    break
                except Exception as e:
                    if retry_count == max_retries - 1:
                        print(f"Failed to upload part {part_number} after {max_retries} attempts: {str(e)}")
                        print(f"You can resume this upload later with the same command.")
                        sys.exit(1)
                    print(f"Upload error, retrying part {part_number} in {retry_delay} seconds... ({retry_count+1}/{max_retries})")
                    time.sleep(retry_delay)
            
            part_number += 1

    # Complete the multipart upload
    s3_client.complete_multipart_upload(
        Bucket=dest_bucket,
        Key=dest_key,
        UploadId=upload_id,
        MultipartUpload={'Parts': sorted(parts, key=lambda p: p['PartNumber'])}
    )
    
    # Verify final file size
    head_response = s3_client.head_object(Bucket=dest_bucket, Key=dest_key)
    if head_response.get('ContentLength') != file_size:
        print(f"WARNING: File size mismatch. Expected: {file_size}, Actual: {head_response['ContentLength']}")
    else:
        print("File size verification successful")
    
    print(f"Successfully transferred file to s3://{dest_bucket}/{dest_key}")

except KeyboardInterrupt:
    progress.close()
    print("\nUpload paused. Run the same command again to resume.")
    print(f"Upload ID: {upload_id}")
    sys.exit(0)
except Exception as e:
    progress.close()
    print(f"Error: {str(e)}")
    print("\nYou can resume this upload later with the same command.")
    sys.exit(1)
finally:
    # Ensure progress bar is closed
    progress.close()