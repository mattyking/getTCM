# Script for debugging the s3 file transfer

# Loading libraries
import boto3
import os
import datetime
import traceback

# Getting today's date
now = datetime.datetime.now()
saveDate = now.strftime("%Y%m%d")

# Upload file to S3 bucket
s3 = boto3.resource('s3')

try:
    s3.Bucket('tcmbooking').upload_file('openings' + saveDate + '.csv')
    os.remove('openings' + saveDate + '.csv')
except Exception:
    traceback.print_exc()

try:    
    s3.Bucket('tcmbooking').upload_file('shifts' + saveDate + '.csv')
    os.remove('shifts' + saveDate + '.csv')
except Exception:
    traceback.print_exc()
        
try:    
    s3.Bucket('tcmbooking').upload_file('error' + saveDate + '.txt')
    os.remove('error' + saveDate + '.txt')
except Exception:
    traceback.print_exc()