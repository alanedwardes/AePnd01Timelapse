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
    filename = FRAMES_OUTPUT + '/' + '{0:03d}'.format(frame) + '.jpg'
    print('Downloading frame ' + str(frame) + ' to ' + filename)
    bucket.download_file(object.key, filename)
  
  params = [FFMPEG, '-framerate 30', '-i ' + FRAMES_OUTPUT + '/%03d.jpg', '-c:v libx264', '-r 30', '-pix_fmt yuv420p', VIDEO_OUTPUT]
  print('Invoking ' + ' '.join(params))
  process = subprocess.Popen(params, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  
  print('ffmpeg stdout: ' + process.stdout.read())
  
  print('ffmpeg stderr: ' + process.stderr.read())
  
  print(os.path.exists(VIDEO_OUTPUT))