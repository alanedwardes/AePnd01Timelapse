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
  objects = bucket.objects.filter(Prefix=PREFIX)
  
  if not os.path.exists(FRAMES_OUTPUT):
    os.makedirs(FRAMES_OUTPUT)
  
  for frame, object in enumerate(objects):
    bucket.download_file(object.key, FRAMES_OUTPUT + '/' + '{0:03d}'.format(frame) + '.jpg')
    
  process = subprocess.Popen([FFMPEG, '-framerate 30', '-i ' + FRAMES_OUTPUT + '/%03d.jpg', '-c:v libx264', '-r 30', '-pix_fmt yuv420p', VIDEO_OUTPUT], shell=True, stdout=subprocess.PIPE)
  print(process.stdout.read())
  print(process.stderr.read())
  
  print os.path.exists(VIDEO_OUTPUT)