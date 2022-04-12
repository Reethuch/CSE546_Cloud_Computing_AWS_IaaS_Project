from flask import Flask, render_template, request, redirect, url_for
import json
import boto3
import boto
import os 
from boto.s3.key import Key
from boto.sqs.message import Message
from time import sleep
import paramiko
import botocore

app = Flask(__name__)

def uploadtoSQSandS3(awsRegion, s3BucketName, sqsQueueName, uploaded_file, s3InputPrefix, s3OutputPrefix,localPath, s3, sqs):
    sqs = boto.sqs.connect_to_region(awsRegion)
    sqsQueue =  sqs.lookup(sqsQueueName)
    #uplaoding the image to S3 input bucket
    print("Sending message to S3 bucket")
    s3.upload_fileobj(uploaded_file, "", str(uploaded_file.filename))
    print("Message is written to the S3 input bucket")
    print("Sending message to SQS queue ...")
    messageBody = json.dumps(['process', s3BucketName, s3InputPrefix, s3OutputPrefix, uploaded_file.filename])
    m = Message()
    m.set_body(messageBody)
    sqsQueue.write(m)
    print("Message is written to the queue!")

@app.route('/', methods=['POST'])
def upload_file():
    aws_region = "us-east-1"
    bucket_name = "" #bucketname
    sqs_queue_name = "" #queuename
    s3 = boto3.client('s3')
    sqs = boto3.resource('sqs')
    for uploaded_file in request.files.getlist('file'):
        if uploaded_file.filename != '':
            uploadtoSQSandS3(aws_region, bucket_name, sqs_queue_name, uploaded_file, "inputfolder", "outputfolder", uploaded_file.filename, s3, sqs)
    return redirect(url_for('ans_index'))


@app.route('/',methods=['GET'])
def ans_index():
    ans = []
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('') #inputbucket name
    for my_bucket_object in my_bucket.objects.all():
        print(my_bucket_object.key)        
        obj = s3.Object('', my_bucket_object.key) #inputbucket name
        print(obj.get()['Body'].read().decode('utf-8'))
        ans.append(obj.get()['Body'].read().decode('utf-8'))
    return render_template('index.html', ans = ans)
