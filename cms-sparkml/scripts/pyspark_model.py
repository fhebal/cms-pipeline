#!/usr/bin/env python
# coding: utf-8

import boto3
import os
import configparser
import findspark
import pyarrow
import uuid
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Log to CloudWatch
def logging(execution_time):
    client = boto3.client('logs')
    stream_name = datetime.now().strftime('%y/%m/%d') + '/predictions/' + \
        str(uuid.uuid1())
    create_stream = client.create_log_stream(
        logGroupName='cms-annual-cost-predictions',
        logStreamName=stream_name
    )
    logEvent = client.put_log_events(
        logGroupName='cms-annual-cost-predictions',
        logStreamName=stream_name,
        logEvents=[
            {
                'timestamp': int(time.time()*1000),
                'message': 'Predictions processed in ' + str(execution_time)
            },
        ]
    )
    return

# Retrieve file names for model training
s3 = boto3.client('s3')
# Beneficiary Summary
files = s3.list_objects(Bucket='cms-data-1', Prefix='Beneficiary_Summary')
beneficiary_files = []
for i in files['Contents']:
    beneficiary_files.append('s3n://cms-data-1/' + i['Key'])

# Inpatient Claims
files = s3.list_objects(Bucket='cms-data-1', Prefix='Inpatient_Claims')
inpatient_files = []
for i in files['Contents']:
    inpatient_files.append('s3n://cms-data-1/' + i['Key'])

# Outpatient Claims
files = s3.list_objects(Bucket='cms-data-1', Prefix='Outpatient_Claims')
outpatient_files = []
for i in files['Contents']:
    outpatient_files.append('s3n://cms-data-1/' + i['Key'])

dir = "Files/"

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"

config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))
config['default']
access_id = config.get('default', "aws_access_key_id")
access_key = config.get('default', "aws_secret_access_key")

findspark.init()
SparkContext.setSystemProperty('spark.driver.memory', '25G')
SparkContext.setSystemProperty('spark.executor.memory', '15G')
conf = SparkConf().setAppName('pyspark_model')
conf = (conf.setMaster('local[*]')
       .set('spark.executor.memory', '15G')
       .set('spark.driver.memory', '25G')
       .set('spark.driver.maxResultSize', '15G'))

sc = SparkContext(conf = conf)
spark = SparkSession(sc)

hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)

def main():
    st_time = datetime.now() #Start time
    # Start model training
    print('Model training has started...')
    beneficiary = spark.read.parquet(*beneficiary_files)
    inpatient = spark.read.parquet(*inpatient_files)
    inpatient = inpatient.fillna({'CLM_FROM_DT': '2008-01-01'})
    outpatient = spark.read.parquet(*outpatient_files)
    beneficiary.printSchema()
    inpatient.printSchema()
    outpatient.printSchema()


    ben = beneficiary.select(
        col('DESYNPUF_ID').alias('PATIENT_ID'),
        'BENE_BIRTH_DT',
        col('BENE_SEX_IDENT_CD').alias("GENDER"),
        col('BENE_RACE_CD').alias('RACE'),
        col('SP_STATE_CODE').alias('STATE'),
       'SP_ALZHDMTA',
       'SP_CHF',
       'SP_CHRNKIDN',
       'SP_CNCR',
       'SP_COPD',
       'SP_DEPRESSN',
       'SP_DIABETES',
       'SP_ISCHMCHT',
       'SP_OSTEOPRS',
       'SP_RA_OA',
       'SP_STRKETIA',
        col('BENRES_IP').alias("ANNUAL_COST"),
    )

    inp = inpatient.groupBy('DESYNPUF_ID').agg(
        count(when(col('ICD9_DGNS_CD_1') != 'nan', True)).alias('DX'),
        count(when(col('ICD9_PRCDR_CD_1') != 'nan', True)).alias('PX'),
        count(when(col('HCPCS_CD_1') != 'nan', True)).alias('HCPCS'),
        max("CLM_FROM_DT").alias("DATE"),
    )

    inner_join = ben.join(inp, ben.PATIENT_ID == inp.DESYNPUF_ID, how='inner')

    timeDiff = (unix_timestamp('DATE', "yyyy-MM-dd HH:mm:ss") - unix_timestamp('BENE_BIRTH_DT', "yyyy-MM-dd HH:mm:ss"))
    inner_join = inner_join.withColumn("AGE_YRS", timeDiff/60/60/24/365)
    df = inner_join.select('ANNUAL_COST','PATIENT_ID','AGE_YRS','GENDER','RACE','STATE','DX','PX','HCPCS', 'SP_ALZHDMTA',
       'SP_CHF', 'SP_CHRNKIDN', 'SP_CNCR', 'SP_COPD', 'SP_DEPRESSN',
       'SP_DIABETES', 'SP_ISCHMCHT', 'SP_OSTEOPRS', 'SP_RA_OA', 'SP_STRKETIA')
    df.filter(col("ANNUAL_COST")!=0).show()


    cat_cols = ['GENDER','RACE','STATE', 'SP_ALZHDMTA',
       'SP_CHF', 'SP_CHRNKIDN', 'SP_CNCR', 'SP_COPD', 'SP_DEPRESSN',
       'SP_DIABETES', 'SP_ISCHMCHT', 'SP_OSTEOPRS', 'SP_RA_OA', 'SP_STRKETIA']
    indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(df) for column in cat_cols ]

    pipeline = Pipeline(stages=indexers)
    indexed = pipeline.fit(df).transform(df)
    indexed.show()


    # creating vectors from features
    # Apache MLlib takes input if vector form
    assembler=VectorAssembler(inputCols=[
        'AGE_YRS',
        'GENDER_index',
        'RACE_index',
        'STATE_index',
        'DX',
        'PX',
        'HCPCS',
        'SP_ALZHDMTA_index',
        'SP_CHF_index',
        'SP_CHRNKIDN_index',
        'SP_CNCR_index',
        'SP_COPD_index',
        'SP_DEPRESSN_index',
        'SP_DIABETES_index',
        'SP_ISCHMCHT_index',
        'SP_OSTEOPRS_index',
        'SP_RA_OA_index',
        'SP_STRKETIA_index'
        ],outputCol='features')
    output=assembler.transform(indexed)
    output.select('features','ANNUAL_COST').show(5)

    # output as below
    train_data, test_data = output.randomSplit([0.7, 0.3])
    test_data.describe().show()

    # creating an object of class LinearRegression
    # object takes features and label as input arguments
    synpuf_lr=LinearRegression(featuresCol='features',labelCol='ANNUAL_COST')

    # pass train_data to train model
    trained_synpuf_model=synpuf_lr.fit(train_data)

    # evaluating model trained for Rsquared error
    synpuf_results=trained_synpuf_model.evaluate(train_data)

    print('Rsquared Error :',synpuf_results.r2)

    unlabeled_data=test_data.select('features')
    unlabeled_data.show(5)

    print('Processing predictions...')
    predictions=trained_synpuf_model.transform(unlabeled_data)
    predictions.show()

    test_df = test_data.toPandas()
    pred_df = predictions.toPandas()


    merged_df = test_df.merge(pred_df)
    merged_df.sort_values(by='prediction', ascending=False, inplace=True)
    merged_df.drop_duplicates(subset=merged_df.columns[1:9], inplace=True)
    merged_df.astype({'features': str, 'DX': 'int32', 'PX': 'int32', 'HCPCS': 'int32'}) \
            .to_parquet(dir + "synpuf_ml_output.parquet", index=False)
    s3.upload_file(dir + "synpuf_ml_output.parquet", "cms-data-1", "Annual_Cost_Predictions/synpuf_ml_output.parquet")
    os.remove(dir + "synpuf_ml_output.parquet")

    print('\n Predictions Completed!')
    fin_time = datetime.now()
    execution_time = fin_time - st_time
    print('\n Total execution time: {0}'.format(str(execution_time)))
    logging(execution_time)

    return

if __name__ == "__main__":

    main()
