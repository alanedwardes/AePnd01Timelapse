import subprocess
import threading
import boto3
import os

FFMPEG = 'ffmpeg/ffmpeg'
BUCKET = 'ae-raspberry'
PREFIX = 'pnd01/photos'
FRAMES_OUTPUT = '/tmp/frames'
VIDEO_OUTPUT = '/tmp/sequence.mp4'

bucket = boto3.resource('s3').Bucket(BUCKET)

def batch(iterable, n):
  l = len(iterable)
  for ndx in range(0, l, n):
    yield iterable[ndx:min(ndx + n, l)]

def download(frame, object):
  if object.size < 1024 * 16:
    print('Skipping frame ' + object.key + ' as it\'s ' + str(object.size) + ' bytes')
    return

  filename = FRAMES_OUTPUT + '/' + '{0:05d}'.format(frame) + '.jpg'
  print('Downloading frame ' + str(frame) + ' to ' + filename)
  bucket.download_file(object.key, filename)

def handler(event, context):
  
  print('Querying bucket for frame objects')
  objects = list(bucket.objects.filter(Prefix=PREFIX).all())
  
  if not os.path.exists(FRAMES_OUTPUT):
    print('Creating frames output directory')
    os.makedirs(FRAMES_OUTPUT)
  
  for object_batch in batch(objects, 10):
    print('Taking frame object batch')
    threads = []
    for frame, object in enumerate(object_batch):
      thread = threading.Thread(target=download, args=(frame, object))
      thread.start()
      threads.append(thread)
    for thread in threads:
      thread.join()

  params = [
    FFMPEG,
    '-y', # Overwrite old files
    '-r', '30',
    '-vcodec', 'mjpeg',
    '-i', FRAMES_OUTPUT + '/%05d.jpg',
    '-vcodec', 'libx264',
    VIDEO_OUTPUT
  ]
  
  print('Invoking ' + ' '.join(params))
  process = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  
  print('ffmpeg stdout: ' + process.stdout.read())
  print('ffmpeg stderr: ' + process.stderr.read())
  
  print('Uploading timelapse S3')
  bucket.upload_file(VIDEO_OUTPUT, 'composite.mp4', Config={'ContentType': 'video/mp4'})