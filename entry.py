import subprocess
import threading
import datetime
import shutil
import boto3
import uuid
import os

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

FFMPEG = 'ffmpeg/ffmpeg'
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

def batch(iterable, n):
  l = len(iterable)
  for ndx in range(0, l, n):
    yield iterable[ndx:min(ndx + n, l)]

def download(frame, object):
  filename = FRAMES_OUTPUT + '/' + '{0:05d}'.format(frame) + '.jpg'
  print('Downloading frame ' + str(frame) + ' to ' + filename)
  bucket.download_file(object.key, filename)

def handler(event, context):
  execute(['rm', TEMP + '/*'])
  
  print('Querying bucket for frame objects')
  objects = list(bucket.objects.filter(Prefix=PREFIX).all())
  
  if not os.path.exists(FRAMES_OUTPUT):
    print('Creating frames output directory')
    os.makedirs(FRAMES_OUTPUT)
  
  frame = 0
  for object_batch in batch(objects, 12):
    print('Taking frame object batch')
    threads = []
    for object in object_batch:
      if os.path.splitext(object.key)[1].lower() not in ['.jpg', '.jpeg']:
        print("Skipping {0}, it's not a frame", object.key)
        continue
      
      frame = frame + 1
      thread = threading.Thread(target=download, args=(frame, object))
      thread.start()
      threads.append(thread)
    for thread in threads:
      thread.join()

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
