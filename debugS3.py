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
    f_name = 'openings' + saveDate + '.csv'
    s3.Bucket('tcmbooking').upload_file(f_name, f_name)
    os.remove('openings' + saveDate + '.csv')
except Exception:
    traceback.print_exc()

try:
    f_name = 'shifts' + saveDate + '.csv'    
    s3.Bucket('tcmbooking').upload_file(f_name, f_name)
    os.remove('shifts' + saveDate + '.csv')
except Exception:
    traceback.print_exc()
        
try:
    f_name = 'error' + saveDate + '.txt'
    s3.Bucket('tcmbooking').upload_file(f_name, f_name)
    os.remove('error' + saveDate + '.txt')
except Exception:
    traceback.print_exc()