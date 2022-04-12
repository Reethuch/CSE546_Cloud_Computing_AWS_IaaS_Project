#!usr/bin/python
import boto3
import json
import paramiko
import threading
from boto.s3.key import Key
from boto.sqs.message import Message
from time import sleep
import boto.s3
import boto.sqs
from sys import argv, exit
import time
import os
import boto
import sys
import subprocess
import signal


instanceIds=[]
instanceCount=0
myInstanceId=''
awsRegion = 'us-east-1'
MASTER_ID = '' #define MasterID
workDir = ''


def getNumberOfInstances(ec2):
    runningInstances = 0
    stoppedInstances = 0
    for inst in ec2.instances.all():
        if((inst.state['Name']=='running' or inst.state['Name']=='pending') and inst.id != MASTER_ID):
            runningInstances+=1
        elif((inst.state['Name']=='stopped' or inst.state['Name']=='stopping')  and inst.id != MASTER_ID):
            stoppedInstances+=1
    return runningInstances, stoppedInstances

def start_instance(no_of_instances):
    for i in range(0,no_of_instances):
        client = boto3.client('ec2')
        response = client.run_instances(
            BlockDeviceMappings=[
                {
                    'DeviceName': '/dev/xvda',
                    'Ebs': {

                        'DeleteOnTermination': True,
                        'VolumeSize': 8,
                        'VolumeType': 'gp2'
                },},
            ],
            ImageId='', #ami instance
            InstanceType='t2.micro',
            KeyName='', #key file name
            MaxCount=1,
            MinCount=1,
            Monitoring={
                'Enabled': False
            },
            SecurityGroupIds=[
               #securityGroupIds
            ],
        )
        instance = response["Instances"][0]
        try:
            client.create_tags(Resources=[instance["InstanceId"]], Tags=[{'Key':'Name', 'Value':'app_tier '+str(i)}])
        except:
            print("couldn't tag because the instance is terminated")
        print(response)

def getRunningInstances(ec2):
    instances = []
    for inst in ec2.instances.all():
        if((inst.state['Name']=='running' or inst.state['Name']=='pending') and inst.id != MASTER_ID):
            instances.append(inst.id)
    return instances

def getStoppedInstances(ec2):
    instances = []
    for inst in ec2.instances.all():
        if((inst.state['Name']=='stopped' or inst.state['Name']=='stopping') and inst.id != MASTER_ID):
            instances.append(inst.id)
    return instances

def processImage(ec2, instance_id):
    key = paramiko.RSAKey.from_private_key_file('') #pem file
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    instance = [i for i in ec2.instances.filter(InstanceIds=[instance_id])][0]
    while(True):
        try:
            client.connect(hostname=instance.public_ip_address, username="ubuntu", pkey=key, timeout=30)
            sin ,sout ,serr = client.exec_command('python3 reciever.py')
            exit_status = sout.channel.recv_exit_status()
            client.close()
            break
        except Exception as e:
            print("Reattempting to connect "+str(e))
            sleep(10)

def getLengthOfQueue(client, sqsQueueUrl):
    response = client.get_queue_attributes(QueueUrl=sqsQueueUrl,AttributeNames=['ApproximateNumberOfMessages',])
    response = int(response['Attributes']['ApproximateNumberOfMessages'])
    return response

def main():
    sqsQueueUrl = '' #QueueURL 
    awsRegion = 'us-east-1'
    ec2 = boto3.resource('ec2')
    client = boto3.client('sqs')
    threshold = 19
    threads = []
    busyInstances = []

    while True:
        print("--------------------------------IN WHILE LOOP----------------------------------------")
        # Get the length of the sqs queue
        qLength = getLengthOfQueue(client, sqsQueueUrl)
        running, stopped = getNumberOfInstances(ec2)
        print(qLength, running, stopped)

        if qLength<=stopped:
            stoppedIds = getStoppedInstances(ec2)  # Get a list of stopped instance ids
            start = min(stopped, qLength - (running - len(busyInstances)))
            if start != 0:
                ec2.instances.filter(InstanceIds=stoppedIds[:start]).start()
                print("Started " + str(stoppedIds[:nStart]) + " instances")
                time.sleep(60)

        else:
            no_of_instances_to_start = min(threshold-(running+stopped),qLength-stopped)
            start_instance(no_of_instances_to_start)
            print("Started " + str(no_of_instances_to_start)+ " new instances")
            time.sleep(60)
            remaining = qLength -  no_of_instances_to_start
            stoppedIds = getStoppedInstances(ec2)  # Get a list of stopped instance ids
            nStart = min(remaining,len(stoppedIds))
            if nStart != 0:
                ec2.instances.filter(InstanceIds=stoppedIds[:nStart]).start()
                print("Started " + str(stoppedIds[:nStart]) + " instances")
                time.sleep(60)

        for runningId in getRunningInstances(ec2):
            if runningId not in busyInstances:
                t = threading.Thread(name=runningId, target=processImage, args=(ec2, runningId))
                threads.append(t)
                busyInstances.append(runningId)
                t.start()
        updated_threads = []
        for t in threads:
            if not t.is_alive():
                busyInstances.remove(t.getName())
            else:
                updated_threads.append(t)

        threads = updated_threads
        runningIds = getRunningInstances(ec2)
        idleIds = [id for id in runningIds if id not in busyInstances]
        if len(idleIds) != 0:
            ec2.instances.filter(InstanceIds=idleIds[:len(idleIds)]).stop()
            print("Stopped " + str(len(idleIds))+ " instances" + str(idleIds[:len(idleIds)]))
        sleep(60)
        length =  getLengthOfQueue(client, sqsQueueUrl)
        if(length == 0):
            sleep(10)
        else:
            sleep(30)

if __name__ == '__main__':
    main()



