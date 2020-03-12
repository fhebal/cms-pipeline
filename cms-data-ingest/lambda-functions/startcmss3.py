######################################################
# File: startcmss3.py
# Description: Python script to start script to scrape
# files from CMS website, process files and convert to
# parquet and upload to S3
# Date: 03/2/2020
######################################################
import time
import boto3
import paramiko
import json

# Script will start file processing script through Lambda function
# triggered by cron that starts ec2 instance

def lambda_handler(event, context):
    ec2 = boto3.resource('ec2', region_name='us-east-1')

    print('Connecting to ec2 instance')
    # Sleep for 90 seconds to give time for ec2 to launch
    time.sleep(90)

    # Get instance ip
    instance_id = 'i-0f9a14fb4d50a61a3'
    instance = ec2.Instance(instance_id)
    instance_ip = instance.public_ip_address

    print('Downloading key')
    # Download key to temporary directory
    s3_client = boto3.client('s3')
    s3_client.download_file('cms-keys', 'project-436.pem', '/tmp/keyname.pem' )

    time.sleep(20)

    # Connect to ec2 instance
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    privkey = paramiko.RSAKey.from_private_key_file('/tmp/keyname.pem')

    ssh.connect(
        instance_ip, username='ec2-user', pkey=privkey
        )
    print('Connected to {0}'.format(instance_ip))

    # Start Docker service and execute python script to process files
    command = '''sudo service docker start &&
        sudo docker run -it ce9c22d94028 python3 /processing/cmss3.py /bin/bash'''
    try:
        transport = ssh.get_transport()
        session = transport.open_session()
        session.set_combine_stderr(True)
        session.get_pty()
        print('Executing {}'.format(command))
        session.exec_command(command)

        time.sleep(310)

        ssh.close()

        return
        {
            'message': 'Script executed successfully'
        }
    except:
        return
        {
            'message': 'Script failed'
        }
