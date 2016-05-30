import subprocess
import threading
import datetime
import shutil
import boto3
import uuid
import os

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

FFMPEG = 'ffmpeg/ffmpeg'
AWS = '/usr/bin/aws'
BUCKET = 'ae-raspberry'
TOPIC = 'arn:aws:sns:eu-west-1:687908690092:AePnd01'
PREFIX = 'pnd01/curated/' + yesterday.strftime('%d-%b-%Y')
TEMP = '/tmp'
FRAMES_OUTPUT = TEMP + '/frames'
VIDEO_OUTPUT = TEMP + '/sequence.mp4'

client = boto3.client('s3')
sns = boto3.resource('sns')
topic = boto3.resource('sns').Topic(TOPIC)
bucket = boto3.resource('s3').Bucket(BUCKET)

def execute(params):
  print('Invoking ' + ' '.join(params))
  process = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  
  stdout = process.stdout.read()
  print('stdout: ' + stdout)
  
  stderr = process.stderr.read()
  print('stderr: ' + stderr)
  
  return stdout

def handler(event, context):
  execute(['rm', TEMP + '/*'])
  execute([AWS, 's3', 'cp', 's3://' + BUCKET + '/' + PREFIX, FRAMES_OUTPUT, '--recursive', '--exclude', '*', '--include', '*.jpg'])

  execute([
    FFMPEG,
    '-r', '10',                         # frame rate
    '-vcodec', 'mjpeg',                 # input is jpeg
    '-i', FRAMES_OUTPUT + '/%05d.jpg',  # frame dir
    '-preset', 'veryfast',              # encode fast, not small
    '-c:v', 'libx264',                  # codec
    '-pix_fmt', 'yuv420p',              # apple preset
    '-profile:v', 'main',               # apple preset
    '-level', '3.1',                    # apple preset
    '-err_detect', 'explode',           # blow up on errors
    VIDEO_OUTPUT
  ])
  
  timelapse = PREFIX + '/' + uuid.uuid4().hex + '.mp4'
  
  print('Uploading timelapse S3')
  print(client.upload_file(VIDEO_OUTPUT, BUCKET, timelapse, ExtraArgs={
    'ContentType': 'video/mp4',
    'ACL': 'public-read'
  }))
  
  # clean up
  execute(['rm', TEMP + '/*'])
  
  #topic.publish(
  #    Message='https://{0}.s3.amazonaws.com/{1}'.format(BUCKET, timelapse),
  #    Subject='Pond Timelapse Available for ' + yesterday.strftime('%A %d %b %Y')
  #)
