import boto3
import boto.s3
import boto
import json
from sys import argv, exit
from boto.s3.key import Key
import boto.sqs
from boto.sqs.message import Message
import os

import subprocess
import signal



def processImagesfromSQS():
  awsRegion = "us-east-1"
  s3 = boto.s3.connect_to_region(awsRegion)
  sqs = boto.sqs.connect_to_region(awsRegion)
  sqsQueue =  sqs.lookup(sqsQueueName)
  print("Getting messages from SQS queue...")
  messages = sqsQueue.get_messages(wait_time_seconds=20)
  workDir = "classifier"
  if messages:
      for m in messages:
          job = json.loads(m.get_body())
          m.delete()
          action = job[0]
          if action == 'process':
              s3BucketName = job[1]
              s3Inputfolder = job[2]
              s3OutputFolder = job[3]
              fileName = job[4]
              status = processImageandSavetoS3(s3, s3BucketName, s3Inputfolder, s3OutputFolder, fileName, workDir)
              if (status):
                  print("Message processed correctly ...")
  else:
      print("No Messages")

def processImageandSavetoS3(s3, s3BucketName, s3Inputfolder, s3OutputFolder, fileName, workDir):
    file_type = fileName.split(".")[1]
    k=4
    if(file_type=="jpeg" or file_type=="JPEG"):
        k=5
    elif (file_type == "png" or file_type=="PNG"):
        k=4
    s3BucketInput = s3.get_bucket(s3BucketName+"-"+s3Inputfolder)
    s3BucketOutput = s3.get_bucket(s3BucketName+"-"+s3OutputFolder)
    downloadInputPath = os.path.join(workDir, fileName)
    downloadOutputPath =  os.path.join(workDir, fileName[:-k]+'.txt')
    remoteInputPath = fileName
    remoteOutputPath =  fileName[:-k]+'.txt'
    if not os.path.isdir(workDir):
        os.system('sudo mkdir work && sudo chmod 777 work')
    key = s3BucketInput.get_key(remoteInputPath)
    s3 = boto3.client('s3')
    s3.download_file(s3BucketName+"-"+"inputfolder", remoteInputPath, downloadInputPath)
    key.get_contents_to_filename(workDir+"/"+fileName)
    os.system('python3 classifier/image_classification.py '+downloadInputPath+' > '+downloadOutputPath)
    with open(downloadOutputPath) as f:
        content = f.readlines()
    with open(downloadOutputPath, "w") as f:
        f.write(str(remoteInputPath)+":"+content[0])
    key = Key(s3BucketOutput)
    key.key = remoteOutputPath
    key.set_contents_from_filename(downloadOutputPath)
    return True

if __name__ == '__main__':
    processImagesfromSQS()
                       