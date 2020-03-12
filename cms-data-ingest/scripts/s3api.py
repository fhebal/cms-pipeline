##################################################
# File: s3api.py
# Description: Python script to connect and perform operations in AWS S3
# List of operations
#   1. List buckets
#   2. Create bucket
#   3. Delete bucket
#   4. Bucket exists
#   5. List Objects
#   6. Create folder
#   7. Delete folder
#   8. Folder exists
#   9. Upload file
#  10. Download file
#  11. Delete File
#  12. File exists
####################################################

import os
import boto3
import logging
from botocore.exceptions import ClientError
from argparse import ArgumentParser
from argparse import RawTextHelpFormatter


def list_buckets():
    """List all buckets in S3"""
    s3_client = boto3.client('s3')

    # Call s3 to list all buckets
    response = s3_client.list_buckets()

    buckets = []
    for bucket_name in response['Buckets']:
        buckets.append(bucket_name['Name'])

    return buckets

def create_bucket(bucket_name, region=None):
    """Create bucket in specific region

    If region is not specified bucket is created in
    default region from credentials file in ~/.aws/config

    Parameters:
    bucket_name: Bucket name to create
    region: region to create bucket (i.e. 'us-east-1')

    Return True if bucket is created or False if not
    """
    try:
        if region is None:
            session = boto3.Session(profile_name='default')
            region = session.region_name
            if region == 'us-east-1':
                s3_client = boto3.client('s3')
                response = s3_client.create_bucket(Bucket=bucket_name)
                logging.info('Created bucket {0} in S3 default region {1} on {2}'.format(bucket_name, region,
                response['ResponseMetadata']['HTTPHeaders']['date']))
            else:
                location = {'LocationConstraint': region}
                s3_client = session.client('s3')
                response = s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
                logging.info('Created bucket {0} in the S3 default region {1} on {2}'.format(bucket_name, region,
                response['ResponseMetadata']['HTTPHeaders']['date']))
        elif region == 'us-east-1':
            s3_client = boto3.client('s3')
            response = s3_client.create_bucket(Bucket=bucket_name)
            logging.info('Created bucket {0} in the S3 region {1} on {2}'.format(bucket_name, region,
            response['ResponseMetadata']['HTTPHeaders']['date']))
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            response = s3_client.create_bucket(Bucket=bucket_name,
                             CreateBucketConfiguration=location)
            logging.info('Created bucket {0} in the S3 region {1} on {2}'.format(bucket_name, region,
            response['ResponseMetadata']['HTTPHeaders']['date']))

        return True

    except ClientError as error:
        logging.error(error)
        return False

def delete_bucket(bucket_name):
    """Deletes and empty s3 bucket

    If bucket is not empty operation will not complete

    Parameters:
    bucket_name: Name of s3 bucket

    Return True if bucket was deleted or False if not
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.delete_bucket(Bucket=bucket_name)
        logging.info('Bucket {0} has been deleted on {1}'.format(bucket_name,
        response['ResponseMetadata']['HTTPHeaders']['date']))
        return True

    except ClientError as error:
        logging.error(error)
        return False

def bucket_exists(bucket_name):
    """See if bucket exists and if the user has
    permission to access the bucket

    Parameters:
    bucket_name: Name of s3 bucket to search

    Return True if bucket exists or False if not
    """
    s3_client = boto3.client('s3')
    s3 = boto3.client('s3')
    try:
        response = s3_client.head_bucket(Bucket=bucket_name)
        logging.info('{0} exists and you have permission to access it.'.format(bucket_name))
        return True

    except ClientError as error:
        logging.debug(error)
        logging.info('{0} does not exist or you do not have permission to access it.'.format(bucket_name))
        return False

def list_objects(bucket_name, object_name=None):
    """List all directories and files with object name from s3 bucket

    Parameters:
    bucket_name: Name of s3 bucket
    object_name: Name of object

    Return list of all directories and files under object name
    """
    objects = []
    if object_name is None:
        object_name = ''

    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=object_name)
        try:
            for object in response['Contents']:
                objects.append(object['Key'])
            logging.info('List of objects found in {0} under {1}'.format(object_name, bucket_name))

        except:
            logging.info('No objects found in {0} under {1}'.format(object_name, bucket_name))
            pass

        return objects

    except ClientError as error:
        logging.error(error)
    return False

def create_folder(bucket_name, folder_name, object_name=None):
    """Create a directory or folder with object or bucket name in s3

    Parameters:
    bucket_name: Name of bucket
    folder_name: Directory or folder to be created under object or bucket name.
                if object name is None then directory will be created in bucket
    object_name: Directory name. If none then folder name is used

    Return True if the directory or folder is created or False if not
    """
    s3_client = boto3.client('s3')
    try:
        if object_name:
            object_name = object_name + folder_name + '/'
        else:
            object_name = folder_name + '/'

        s3_client.put_object(Bucket=bucket_name, Key=object_name)
        logging.info('Created {0} in {1}'.format(object_name, bucket_name))
        return True

    except ClientError as error:
        logging.error(error)
        return False

def delete_folder(bucket_name, object_name):
    """Delete an object, directory, or folder from an s3 bucket

    Parameters:
    bucket_name: Name of bucket
    object_name: Object, directory, or folder

    Return True if object, directory, or folder was deleted or False if not
    """


    if object_name[-1] != '/':
        object_name += '/'

    objects = []
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=object_name)
        try:
            for object in response['Contents']:
                objects.append(object['Key'])
        except:
            pass

        for object in objects:
            s3_client.delete_object(Bucket=bucket_name, Key=object)

        logging.info('{0} was deleted from {1}'.format(object_name, bucket_name))
        return True

    except ClientError as error:
        logging.error(error)
        return False

def folder_exists(bucket_name, object_name=None):
    """Check if object, directory, or folder exists in an s3 bucket

    Parameters:
    bucket_name: Name of bucket
    object_name: s3 object, directory, or file name to be searched.
                If none, error

    Return True if found or False if not
    """

    objects = []

    if object_name is None:
        logging.error('Folder name (object_name) cannot be empty'.format(object_name, bucket_name))
        return False

    else:
        s3_client = boto3.client('s3')
        try:
            response = s3_client.list_objects(Bucket=bucket_name, Prefix=object_name)
            try:
                for object in response['Contents']:
                    objects.append(object['Key'])
            except:
                pass
            if objects != []:
                logging.info('{0} found in {1}'.format(object_name, bucket_name))
                return True
            else:
                logging.info('{0} not found in {1}'.format(object_name, bucket_name))
                return False

        except ClientError as error:
            logging.error(error)
            return False

def upload_file(bucket_name, dir, file_name, object_name=None):
    """Upload a file to a s3 bucket

    Parameters:
    bucket_name: Bucket to upload to
    dir: Local directory from where to file to read for upload
    file_name: File to upload
    object_name: AWS S3 directory name. If not specified then same as file_name

    Return True if file was uploaded or False if not
    """

    # If S3 object_name was not specified, use file_name
    if object_name:
        object_name += file_name
    else:
        object_name = file_name

    file_name = dir + file_name

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
        logging.info('File {0} uploaded successfully'.format(file_name))
        return True

    except ClientError as error:
        logging.error(error)
        return False

def download_file(bucket_name, dir, file_name, object_name=None):
    """Download a file from a s3 bucket

    Parameters:
    bucket_name: Bucket to download from
    dir: Local directory to save the file
    file_name: File to download
    object_name: AWS S3 object, directory, or folder name.
                 If not specified then same as file name

    Return True if file was downloaded or False if not
    """

    # If S3 object_name was not specified, use file_name
    if object_name:
        object_name += file_name
    else:
        object_name = file_name

    if dir:
        if not os.path.exists(dir):
            os.makedirs(dir)

        file_name = dir + file_name

    s3_client = boto3.resource('s3')
    try:
        s3_client.Bucket(bucket_name).download_file(object_name, file_name)
        logging.info('Successfully downloaded {0} from {1}'.format(object_name, bucket_name))
        return True

    except ClientError as error:
        if error.response['Error']['Code'] == "404":
            logging.error("The file does not exist.")
        else:
            logging.error(error)

        return False

def delete_file(bucket_name, file_name, object_name=None):
    """Delete a file from a S3 bucket

    Parameters:
    bucket_name: Name of bucket
    file_name: File to delete
    object_name: AWS S3 object, directory, or folder name.
                 If not specified then same as file name

    Return True if file was deleted or False if not
    """

    orig_file_name = file_name
    orig_object_name = object_name

    # If S3 object_name was not specified, use file_name
    if object_name:
        object_name += file_name
    else:
        object_name = file_name

    s3_client = boto3.client('s3')
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)

        if file_exists(bucket_name, orig_file_name, orig_object_name):
            logging.error('Problem deleting {0} from {1}'.format(object_name, bucket_name))
            return False
        else:
            logging.info('{0} was deleted from {1}'.format(object_name, bucket_name))
            return True

    except ClientError as error:
        logging.error(error)
        return False

def file_exists(bucket_name, file_name, object_name=None):
    """Check if file exists in a s3 bucket

    Parameters:
    bucket_name: Name of bucket
    file_name: File to check
    object_name: s3 object, directory, or file name to be searched.
                If not specified then same as file name

    Return True if found or False if not
    """

    objects = []

    # If s3 object_name was not specified, use file_name
    if object_name:
        object_name += file_name
    else:
        object_name = file_name

    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=object_name)
        try:
            for object in results['Contents']:
                objects.append(object['Key'])
        except:
            pass

        if objects != []:
            logging.info('{0} found in {1}'.format(object_name, bucket_name))
            return True
        else:
            logging.info('{0} not found in {1}'.format(object_name, bucket_name))
            return False

    except ClientError as error:
        logging.error(error)
        return False

def main():

    parser = ArgumentParser(formatter_class=RawTextHelpFormatter, prog='s3api.py', description='Connect to S3 and perform operations. \n ')

    parser.add_argument('-b', '--bucket_name', dest='bucket_name', help='S3 bucket name\n\n', required=False, default=None)
    parser.add_argument('-o', '--object_name', dest='object_name', help='S3 directory name\n\n', required=False, default=None)
    parser.add_argument('-ld', '--dir', dest='dir', help='Local directory for downloading files & uploading to S3\n\n', required=False, default=None)
    parser.add_argument('-f', '--file_name', dest='file_name', help='File name\n\n', required=False, default=None)
    parser.add_argument('-r', '--region', dest='region', help='S3 bucket region for bucket creation\n\n', required=False, default=None)
    parser.add_argument('-rd', '--remote_dir', dest='remote_dir', help='S3 directory to be created\n\n', required=False, default=None)
    parser.add_argument('-l', '--log_level', dest='log_lvl', choices=['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Log level to create logs\n\n', default='WARNING')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-bo', '--bucket_operation', dest='bucket_operation', choices=['list', 'create', 'delete', 'exists'], help='Perform bucket operation\n\n')
    group.add_argument('-oo', '--object_operation', dest='object_operation', choices=['list', 'create', 'delete', 'exists'], help='Perform object/folder operation\n\n')
    group.add_argument('-fo', '--file_operation', dest='file_operation', choices=['upload', 'download', 'delete', 'exists'], help='Perform file operation\n\n')

    args = parser.parse_args()

    bucket_name      = args.bucket_name
    object_name      = args.object_name
    dir              = args.dir
    file_name        = args.file_name
    region           = args.region
    remote_dir       = args.remote_dir
    bucket_operation = args.bucket_operation
    object_operation = args.object_operation
    file_operation   = args.file_operation
    log_lvl          = args.log_lvl

    if log_lvl.upper()== 'NOTSET':
        log_level = logging.NOTSET
    elif log_lvl.upper()== 'DEBUG':
        log_level = logging.DEBUG
    elif log_lvl.upper()== 'INFO':
        log_level = logging.INFO
    elif log_lvl.upper()== 'WARNING':
        log_level = logging.WARNING
    elif log_lvl.upper()== 'ERROR':
        log_level = logging.ERROR
    elif log_lvl.upper()== 'CRITICAL':
        log_level = logging.CRITICAL

    # Set up logging
    logging.basicConfig(level=log_level,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    if bucket_operation in ['create', 'delete', 'exists'] and not(bucket_name):
        parser.error('-b BUCKET_NAME is required with -bo {create, delete, exists} only list can be provided without -b BUCKET_NAME')

    if object_operation in ['list', 'exists'] and not(bucket_name):
        parser.error('-b BUCKET_NAME is required -o OBJECT_NAME is optional with -oo {list, create, exists}')
    elif object_operation == 'create' and (not(bucket_name) or not(remote_dir)):
        parser.error('-b BUCKET_NAME -rd REMOTE_DIR are required -o OBJECT_NAME is optional with -oo {create}')
    elif object_operation == 'delete' and (not(bucket_name) or not(object_name)):
        parser.error('-b BUCKET_NAME -o OBJECT_NAME are required with -oo {delete}')

    if file_operation in ['upload', 'download'] and (not(bucket_name) or not(dir) or not(file_name)):
        parser.error('-b BUCKET_NAME -ld LOCAL_DIR -f FILE_NAME are required & -o OBJECT_NAME is optional with -fo {upload, download}')
    elif file_operation in ['delete', 'exists'] and (not(bucket_name) or not(file_name)):
        parser.error('-b BUCKET_NAME -f FILE_NAME are required & -o OBJECT_NAME  is optional with -fo {delete, exists}')

    # Add '/' to directory strings if not already present at the end
    if object_name and object_name[-1] != '/':
        object_name += '/'

    if dir and dir[-1] != '/':
        dir += '/'

    print_result = None

    if bucket_operation == 'list':
        print_result = 'Bucket List: {0}'.format(list_buckets())

    if bucket_operation == 'create':
        if create_bucket(bucket_name, region):
            print_result = 'Bucket was successfully created'
        else:
            print_result = 'Bucket creation was not successful'

    if bucket_operation == 'delete':
        if delete_bucket(bucket_name):
            print_result = 'Bucket was successfully deleted'
        else:
            print_result = 'Bucket deletion was not successful'

    if bucket_operation == 'exists':
        if bucket_exists(bucket_name):
            print_result = 'Bucket exists'
        else:
            print_result = 'Bucket was not found / Bucket access denied / Bucket exists operation was not successful - check above for actual cause'

    if object_operation == 'list':

        objects = list_objects(bucket_name, object_name)

        if objects != []:
            print('\n \n Object List: \n')
            for object in objects:
                print(object)
            print('\n \n')
        else:
            print_result = 'No objects found'

    if object_operation == 'create':

        if create_folder(bucket_name, remote_dir, object_name):
            print_result = 'Folder was successfully created'
        else:
            print_result = 'Folder creation was not successful'

    if object_operation == 'delete':

        if delete_folder(bucket_name, object_name):
            print_result = 'Folder was successfully deleted'
        else:
            print_result = 'Folder deletion was not successful'

    if object_operation == 'exists':

        if folder_exists(bucket_name, object_name):
            print_result = 'Folder exists'
        else:
            print_result = 'Folder was not found / Folder exists operation was not successful - check above actual cause'

    if file_operation == 'upload':

        if upload_file(bucket_name, dir, file_name, object_name):
            print_result = 'File was successfully uploaded'
        else:
            print_result = 'File upload was not successful'

    if file_operation == 'download':

        if download_file(bucket_name, dir, file_name, object_name):
            print_result = 'File was successfully downloaded'
        else:
            print_result = 'File download was not successful'

    if file_operation == 'delete':

        if delete_file(bucket_name, file_name, object_name):
            print_result = 'File was successfully deleted'
        else:
            print_result = 'File deletion was not successful'

    if file_operation == 'exists':

        if file_exists(bucket_name, file_name, object_name):
            print_result = 'File exists'
        else:
            print_result = 'File was not found / File exists operation was not successful - check above actual cause'

    if print_result:
        print('\n \n' + print_result + '\n \n')

if __name__ == '__main__':

    main()
