import boto3

BUCKET = 'ae-raspberry'
PREFIX = 'pnd01/photos'

def handler(event, context):
  bucket = s3.Bucket(BUCKET)
  for object in bucket.objects.filter(Prefix=PREFIX):
    print(object)