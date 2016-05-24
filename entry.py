import subprocess
import boto3
import os

FFMPEG = 'ffmpeg/ffmpeg'
BUCKET = 'ae-raspberry'
PREFIX = 'pnd01/photos'
FRAMES_OUTPUT = '/tmp/frames'
VIDEO_OUTPUT = '/tmp/sequence.mp4'

s3 = boto3.resource('s3')

def handler(event, context):
  bucket = s3.Bucket(BUCKET)
  
  print('Querying bucket for frame objects')
  objects = bucket.objects.filter(Prefix=PREFIX)
  
  if not os.path.exists(FRAMES_OUTPUT):
    print('Creating frames output directory')
    os.makedirs(FRAMES_OUTPUT)
  
  print('Looping frame objects')
  for frame, object in enumerate(objects):
    if object.size < 1024 * 16:
      print('Skipping frame ' + object.key + ' as it\'s ' + str(object.size) + ' bytes')
      continue
  
    filename = FRAMES_OUTPUT + '/' + '{0:05d}'.format(frame) + '.jpg'
    print('Downloading frame ' + str(frame) + ' to ' + filename)
    bucket.download_file(object.key, filename)
    
    # only grab 100 while i'm debugging
    if frame > 100:
      break

  params = [
    FFMPEG,
    '-y', # Overwrite old files
    '-r', '1',
    '-vcodec', 'mjpeg',
    '-i', FRAMES_OUTPUT + '/%05d.jpg',
    '-vcodec', 'libx264',
    VIDEO_OUTPUT
  ]
  
  print('Invoking ' + ' '.join(params))
  process = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  
  print('ffmpeg stdout: ' + process.stdout.read())
  
  print('ffmpeg stderr: ' + process.stderr.read())
  
  bucket.upload_file(VIDEO_OUTPUT, 'composite.mp4')