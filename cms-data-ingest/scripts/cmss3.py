##################################################
# File: cmss3.py
# Description: Python script to scrape files from
# CMS website, process files and convert to
# parquet and upload to S3
# Date: 03/2/2020
####################################################
import os
import requests
import zipfile
import pandas as pd
import numpy as np
import pyarrow
import boto3
import uuid
import time
from datetime import datetime
from bs4 import BeautifulSoup
import s3api

# Process script to scape files from CMS website, process files and convert to parquet
# and upload to S3

def tryRequest(url, headers):
    # Ping site until response status is 200, if not print fail
    success = False
    for _ in range(5):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            success = True
            return response
            break
        else:
            print('Response received: %s. Retrying: %s'%(response.status_code, url))
            success = False
    if success == False:
        return print("Failed to process the URL: ", url)

def parseCSV(dir, file_name):
    # Parse CSV files in batches of 100,000
    files = []
    split = 100000
    count = 0
    num = 1
    dest = None
    print('Processing files in batches of {0}...'.format(split))
    f = open(dir + file_name, 'r', encoding='iso-8859-1')
    header = f.readline()
    for line in f:
        if count % split == 0:
            if dest: dest.close()
            dest = open(dir + str(num) + file_name, 'w')
            files.append((dir, str(num) + file_name))
            dest.write(header)
            num += 1
        dest.write(line)
        count += 1
    f.close()
    return files

def logging(count, execution_time):
    client = boto3.client('logs')
    stream_name = datetime.now().strftime('%y/%m/%d') + '/processed/' + \
        str(uuid.uuid1())
    create_stream = client.create_log_stream(
        logGroupName='cmss3-processing',
        logStreamName=stream_name
    )
    logEvent = client.put_log_events(
        logGroupName='cmss3-processing',
        logStreamName=stream_name,
        logEvents=[
            {
                'timestamp': int(time.time()*1000),
                'message': str(count) + ' total files processed in ' + \
                    str(execution_time)
            },
        ]
    )
    return

def main():
    st_time = datetime.now() #Start time
    # Start data download
    print('Data download has started...')
    dir = 'Files/'
    url_path = 'https://www.cms.gov'
    url = 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF'
    headers = {'User-Agent': "Chrome/54.0.2840.90"}
    # Grab all href from file summary page
    response = tryRequest(url, headers)
    html = response.content
    soup = BeautifulSoup(html, 'html.parser')
    tmpRow = soup.findAll('a')
    count = 0
    for i in range(len(tmpRow)):
    # Parse all links to pages that have cms data files to download
        try:
            if tmpRow[i].contents[0].split(' ')[0] == 'DE1.0':
                response = requests.get(url_path + tmpRow[i]['href'], headers=headers)
                html = response.content
                soup = BeautifulSoup(html, 'html.parser')
                tmpUrl = soup.findAll('a')
                for k in range(len(tmpUrl)):
                # Download all files, transform, process into parquet files, and upload to s3 into respective S3 buckets
                # all files in local ec2 instance are removed
                    try:
                        if tmpUrl[k].contents[0].split(' ')[4] == 'Beneficiary':
                            filepath = tmpUrl[k]['href']
                            try:
                                response = tryRequest(url_path + filepath, headers=headers)
                                zip_path = dir + filepath.split('/')[-1]
                                with open(zip_path, 'wb') as f:
                                    f.write(response.content)
                                filename = zipfile.ZipFile(zip_path, 'r').namelist()[0]
                                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                                    zip_ref.extractall(dir)
                                os.remove(zip_path)
                                files = parseCSV(dir, filename)
                                os.remove(dir + filename)
                                for e in range(len(files)):
                                    df = pd.read_csv(dir + files[e][1], low_memory=False)
                                    os.remove(dir + files[e][1])
                                    df['BENE_BIRTH_DT'] = pd.to_datetime(df['BENE_BIRTH_DT'].astype(str), format='%Y%m%d').dt.date
                                    df['BENE_BIRTH_DT'].fillna(np.nan, inplace=True)
                                    df['BENE_DEATH_DT'] = pd.to_datetime(df['BENE_DEATH_DT'].astype(str), format='%Y%m%d').dt.date
                                    df['BENE_DEATH_DT'].fillna(np.nan, inplace=True)
                                    df = df.apply(lambda x: x.astype('int16') if x.dtype == 'int64' else x)
                                    parq_file = files[e][1].split('.')[0] + '.parquet'
                                    df.to_parquet(dir + parq_file)
                                    s3api.upload_file('cms-data-1', dir, parq_file, 'Beneficiary_Summary/')
                                    os.remove(dir + parq_file)
                                    count += 1
                                print('{0} files have been processed into Beneficiary_Summary'.format(len(files)))
                            except:
                                response
                        elif tmpUrl[k].contents[0].split(' ')[-3] == 'Inpatient':
                            filepath = tmpUrl[k]['href']
                            try:
                                response = tryRequest(url_path + filepath, headers=headers)
                                zip_path = dir + filepath.split('/')[-1]
                                with open(zip_path, 'wb') as f:
                                    f.write(response.content)
                                filename = zipfile.ZipFile(zip_path, 'r').namelist()[0]
                                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                                    zip_ref.extractall(dir)
                                os.remove(zip_path)
                                files = parseCSV(dir, filename)
                                os.remove(dir + filename)
                                for e in range(len(files)):
                                    df = pd.read_csv(dir + files[e][1], low_memory=False)
                                    os.remove(dir + files[e][1])
                                    df['CLM_FROM_DT'] = pd.to_datetime(df['CLM_FROM_DT'].astype(str), format='%Y%m%d').dt.date
                                    df['CLM_FROM_DT'].fillna(np.nan, inplace=True)
                                    df['CLM_THRU_DT'] = pd.to_datetime(df['CLM_THRU_DT'].astype(str), format='%Y%m%d').dt.date
                                    df['CLM_THRU_DT'].fillna(np.nan, inplace=True)
                                    df['CLM_ADMSN_DT'] = pd.to_datetime(df['CLM_ADMSN_DT'].astype(str), format='%Y%m%d').dt.date
                                    df['CLM_ADMSN_DT'].fillna(np.nan, inplace=True)
                                    df['NCH_BENE_DSCHRG_DT'] = pd.to_datetime(df['NCH_BENE_DSCHRG_DT'].astype(str), format='%Y%m%d').dt.date
                                    df['NCH_BENE_DSCHRG_DT'].fillna(np.nan, inplace=True)
                                    df['AT_PHYSN_NPI'] = df['AT_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                    df['OP_PHYSN_NPI'] = df['OP_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                    df['OT_PHYSN_NPI'] = df['OT_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                    df['SEGMENT'] = df['SEGMENT'].astype('int16')
                                    df.iloc[:,20:] = df.iloc[:,20:].apply(lambda x: x.astype(str) if x.dtype == 'float' else x)
                                    df['CLM_UTLZTN_DAY_CNT'] = df['CLM_UTLZTN_DAY_CNT'].fillna(0).astype('int16')
                                    parq_file = files[e][1].split('.')[0] + '.parquet'
                                    df.to_parquet(dir + parq_file)
                                    s3api.upload_file('cms-data-1', dir, parq_file, 'Inpatient_Claims/')
                                    os.remove(dir + parq_file)
                                    count += 1
                                print('{0} files have been processed into Inpatient_Claims'.format(len(files)))
                            except:
                                response
                        elif tmpUrl[k].contents[0].split(' ')[-3] == 'Outpatient':
                            filepath = tmpUrl[k]['href']
                            try:
                                response = tryRequest(url_path + filepath, headers=headers)
                                zip_path = dir + filepath.split('/')[-1]
                                with open(zip_path, 'wb') as f:
                                    f.write(response.content)
                                filename = zipfile.ZipFile(zip_path, 'r').namelist()[0]
                                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                                    zip_ref.extractall(dir)
                                os.remove(zip_path)
                                files = parseCSV(dir, filename)
                                os.remove(dir + filename)
                                for e in range(len(files)):
                                    df = pd.read_csv(dir + files[e][1], low_memory=False)
                                    os.remove(dir + files[e][1])
                                    df['CLM_FROM_DT'] = pd.to_datetime(df['CLM_FROM_DT'].astype(str), format='%Y%m%d').dt.date
                                    df['CLM_FROM_DT'].fillna(np.nan, inplace=True)
                                    df['CLM_THRU_DT'] = pd.to_datetime(df['CLM_THRU_DT'].astype(str), format='%Y%m%d').dt.date
                                    df['CLM_THRU_DT'].fillna(np.nan, inplace=True)
                                    df['SEGMENT'] = df['SEGMENT'].astype('int16')
                                    df['AT_PHYSN_NPI'] = df['AT_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                    df['OP_PHYSN_NPI'] = df['OP_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                    df['OT_PHYSN_NPI'] = df['OT_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                    df.iloc[:,12:28] = df.iloc[:,12:28].apply(lambda x: x.astype(str) if x.dtype == 'float' else x)
                                    df.iloc[:,31:] = df.iloc[:,31:].apply(lambda x: x.astype(str) if x.dtype == 'float' else x)
                                    parq_file = files[e][1].split('.')[0] + '.parquet'
                                    df.to_parquet(dir + parq_file)
                                    s3api.upload_file('cms-data-1', dir, parq_file, 'Outpatient_Claims/')
                                    os.remove(dir + parq_file)
                                    count += 1
                                print('{0} files have been processed into Outpatient_Claims'.format(len(files)))
                            except:
                                response
                        elif tmpUrl[k].contents[0].split(' ')[-3] == 'Prescription':
                            filepath = tmpUrl[k]['href']
                            try:
                                response = tryRequest(filepath, headers=headers)
                                zip_path = dir + filepath.split('/')[-1]
                                with open(zip_path, 'wb') as f:
                                    f.write(response.content)
                                filename = zipfile.ZipFile(zip_path, 'r').namelist()[0]
                                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                                    zip_ref.extractall(dir)
                                os.remove(zip_path)
                                files = parseCSV(dir, filename)
                                os.remove(dir + filename)
                                for e in range(len(files)):
                                    df = pd.read_csv(dir + files[e][1], low_memory=False)
                                    os.remove(dir + files[e][1])
                                    df['SRVC_DT'] = pd.to_datetime(df['SRVC_DT'].astype(str), format='%Y%m%d').dt.date
                                    df['SRVC_DT'].fillna(np.nan, inplace=True)
                                    df['PROD_SRVC_ID'] = df['PROD_SRVC_ID'].astype(str) 
                                    df['DAYS_SUPLY_NUM'] = df['DAYS_SUPLY_NUM'].astype('int16')
                                    df['QTY_DSPNSD_NUM'] = df['QTY_DSPNSD_NUM'].fillna(0).astype('int16')
                                    parq_file = files[e][1].split('.')[0] + '.parquet'
                                    df.to_parquet(dir + parq_file)
                                    s3api.upload_file('cms-data-1', dir, parq_file, 'Prescription_Drug_Events/')
                                    os.remove(dir + parq_file)
                                    count += 1
                                print('{0} files have been processed into Prescription_Drug_Events'.format(len(files)))
                            except:
                                response
                    except:
                        try:
                            if tmpUrl[k].contents[1].split('(', 1)[1].split(')')[0] == 'ZIP':
                                filepath = tmpUrl[k]['href']
                                try:
                                    response = tryRequest(url_path + filepath, headers=headers)
                                    zip_path = dir + filepath.split('/')[-1]
                                    with open(zip_path, 'wb') as f:
                                        f.write(response.content)
                                    filename = zipfile.ZipFile(zip_path, 'r').namelist()[0]
                                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                                        zip_ref.extractall(dir)
                                    os.remove(zip_path)
                                    files = parseCSV(dir, filename)
                                    os.remove(dir + filename)
                                    for e in range(len(files)):
                                        df = pd.read_csv(dir + files[e][1], low_memory=False)
                                        os.remove(dir + files[e][1])
                                        if files[e][1].split('_')[5] == 'Inpatient':
                                            df['CLM_FROM_DT'] = pd.to_datetime(df['CLM_FROM_DT'].astype(str), format='%Y%m%d').dt.date
                                            df['CLM_FROM_DT'].fillna(np.nan, inplace=True)
                                            df['CLM_THRU_DT'] = pd.to_datetime(df['CLM_THRU_DT'].astype(str), format='%Y%m%d').dt.date
                                            df['CLM_THRU_DT'].fillna(np.nan, inplace=True)
                                            df['CLM_ADMSN_DT'] = pd.to_datetime(df['CLM_ADMSN_DT'].astype(str), format='%Y%m%d').dt.date
                                            df['CLM_ADMSN_DT'].fillna(np.nan, inplace=True)
                                            df['NCH_BENE_DSCHRG_DT'] = pd.to_datetime(df['NCH_BENE_DSCHRG_DT'].astype(str), format='%Y%m%d').dt.date
                                            df['NCH_BENE_DSCHRG_DT'].fillna(np.nan, inplace=True)
                                            df['AT_PHYSN_NPI'] = df['AT_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                            df['OP_PHYSN_NPI'] = df['OP_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                            df['OT_PHYSN_NPI'] = df['OT_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                            df['SEGMENT'] = df['SEGMENT'].astype('int16')
                                            df.iloc[:,20:] = df.iloc[:,20:].apply(lambda x: x.astype(str) if x.dtype == 'float' else x)
                                            df['CLM_UTLZTN_DAY_CNT'] = df['CLM_UTLZTN_DAY_CNT'].fillna(0).astype('int16')
                                            parq_file = files[e][1].split('.')[0] + '.parquet'
                                            df.to_parquet(dir + parq_file)
                                            s3api.upload_file('cms-data-1', dir, parq_file, 'Inpatient_Claims/')
                                            count += 1
                                        elif files[e][1].split('_')[5] == 'Outpatient':
                                            df['CLM_FROM_DT'] = pd.to_datetime(df['CLM_FROM_DT'].astype(str), format='%Y%m%d').dt.date
                                            df['CLM_FROM_DT'].fillna(np.nan, inplace=True)
                                            df['CLM_THRU_DT'] = pd.to_datetime(df['CLM_THRU_DT'].astype(str), format='%Y%m%d').dt.date
                                            df['CLM_THRU_DT'].fillna(np.nan, inplace=True)
                                            df['SEGMENT'] = df['SEGMENT'].astype('int16')
                                            df['AT_PHYSN_NPI'] = df['AT_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                            df['OP_PHYSN_NPI'] = df['OP_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                            df['OT_PHYSN_NPI'] = df['OT_PHYSN_NPI'].fillna(-1).astype('int64').astype(str).replace('-1', np.nan)
                                            df.iloc[:,12:28] = df.iloc[:,12:28].apply(lambda x: x.astype(str) if x.dtype == 'float' else x)
                                            df.iloc[:,31:] = df.iloc[:,31:].apply(lambda x: x.astype(str) if x.dtype == 'float' else x)
                                            parq_file = files[e][1].split('.')[0] + '.parquet'
                                            df.to_parquet(dir + parq_file)
                                            s3api.upload_file('cms-data-1', dir, parq_file, 'Outpatient_Claims/')
                                            count += 1
                                        elif files[e][1].split('_')[3] == 'Beneficiary':
                                            df['BENE_BIRTH_DT'] = pd.to_datetime(df['BENE_BIRTH_DT'].astype(str), format='%Y%m%d').dt.date
                                            df['BENE_BIRTH_DT'].fillna(np.nan, inplace=True)
                                            df['BENE_DEATH_DT'] = pd.to_datetime(df['BENE_DEATH_DT'].astype(str), format='%Y%m%d').dt.date
                                            df['BENE_DEATH_DT'].fillna(np.nan, inplace=True)
                                            df = df.apply(lambda x: x.astype('int16') if x.dtype == 'int64' else x)
                                            parq_file = files[e][1].split('.')[0] + '.parquet'
                                            df.to_parquet(dir + parq_file)
                                            s3api.upload_file('cms-data-1', dir, parq_file, 'Beneficiary_Summary/')
                                            count += 1
                                        os.remove(dir + parq_file)
                                    if files[0][1].split('_')[5] == 'Inpatient':
                                        print('{0} files have been processed into Inpatient_Claims'.format(len(files)))
                                    elif files[0][1].split('_')[5] == 'Outpatient':
                                        print('{0} files have been processed into Outpatient_Claims'.format(len(files)))
                                    elif files[0][1].split('_')[3] == 'Beneficiary':
                                        print('{0} files have been processed into Beneficiary_Summary'.format(len(files)))
                                except:
                                    response
                        except:
                            pass
        except:
            pass

    # Display completion
    print('\n Processing Completed!')
    print('{0} total files processed'.format(count))
    fin_time = datetime.now()
    execution_time = fin_time - st_time
    print('\n Total execution time: {0}'.format(str(execution_time)))
    logging(count, execution_time)
    return

if __name__ == "__main__":

    main()
