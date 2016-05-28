import subprocess
import threading
import datetime
import boto3
import os

yesterday = datetime.datetime.now() #- datetime.timedelta(days=1)

FFMPEG = 'ffmpeg/ffmpeg'
BUCKET = 'ae-raspberry'
TOPIC = 'arn:aws:sns:eu-west-1:687908690092:AePnd01'
PREFIX = os.path.join('pnd01/curated', yesterday.strftime('%d-%b-%Y'))
FRAMES_OUTPUT = '/tmp/frames'
VIDEO_OUTPUT = '/tmp/sequence.mp4'
LINK_VALIDITY_DAYS = 7

client = boto3.client('s3')
sns = boto3.resource('sns')
topic = boto3.resource('sns').Topic(TOPIC)
bucket = boto3.resource('s3').Bucket(BUCKET)

def batch(iterable, n):
  l = len(iterable)
  for ndx in range(0, l, n):
    yield iterable[ndx:min(ndx + n, l)]

def download(frame, object):
  filename = FRAMES_OUTPUT + '/' + '{0:05d}'.format(frame) + '.jpg'
  print('Downloading frame ' + str(frame) + ' to ' + filename)
  bucket.download_file(object.key, filename)

def handler(event, context):
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
      frame = frame + 1
      thread = threading.Thread(target=download, args=(frame, object))
      thread.start()
      threads.append(thread)
    for thread in threads:
      thread.join()

  params = [
    FFMPEG,
    '-y', # Overwrite old files
    '-r', '20',
    '-vcodec', 'mjpeg',
    '-i', FRAMES_OUTPUT + '/%05d.jpg',
    '-vcodec', 'libx264',
    VIDEO_OUTPUT
  ]
  
  print('Invoking ' + ' '.join(params))
  process = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  
  print('ffmpeg stdout: ' + process.stdout.read())
  print('ffmpeg stderr: ' + process.stderr.read())
  
  timelapse = os.path.join(PREFIX, 'timelapse.mp4')
  
  print('Uploading timelapse S3')
  client.upload_file(VIDEO_OUTPUT, BUCKET, timelapse, ExtraArgs={'ContentType': 'video/mp4'})
  
  url = client.generate_presigned_url('get_object', Params = { 'Bucket': BUCKET, 'Key': timelapse }, ExpiresIn = 60 * 60 * 24 * LINK_VALIDITY_DAYS)
  
  topic.publish(
      Message=url,
      Subject='Pond Timelapse Available for ' + yesterday.strftime('%A %d %b %Y')
  )