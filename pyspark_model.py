#!/usr/bin/env python
# coding: utf-8

import boto3
import os
import configparser
import findspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors 
from pyspark.ml.feature import VectorAssembler 
from pyspark.ml.regression import LinearRegression

# Retrieve the list of existing buckets
s3 = boto3.client('s3')
response = s3.list_buckets()

# Output the bucket names
print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')

# print bucket contents
bucket = s3.list_objects(Bucket='cms-data-1')
for i in bucket['Contents']:
    print(i['Key'])

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"

config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))
config['default']
access_id = config.get('default', "aws_access_key_id") 
access_key = config.get('default', "aws_secret_access_key")

findspark.init()
sc = SparkContext('local')
spark = SparkSession(sc)

sc=spark.sparkContext
hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)

beneficiary_1 = spark.read.parquet("s3n://cms-data-1/Beneficiary_Summary/1DE1_0_2008_Beneficiary_Summary_File_Sample_1.parquet")
beneficiary_1 = beneficiary_1.withColumn('BENE_BIRTH_DT',to_date(col('BENE_BIRTH_DT').cast('string'),format='yyyyMMdd'))
inpatient_1 = spark.read.parquet("s3n://cms-data-1/Inpatient_Claims/1DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.parquet")
inpatient_1 = inpatient_1.withColumn('CLM_FROM_DT',to_date(lit('20080101'),'yyyyMMdd'))
outpatient_1 = spark.read.parquet("s3n://cms-data-1/Outpatient_Claims/1DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.parquet")
beneficiary_1.printSchema()
inpatient_1.printSchema()
outpatient_1.printSchema()


ben = beneficiary_1.select(
    col('DESYNPUF_ID').alias('PATIENT_ID'),
    'BENE_BIRTH_DT',
    col('BENE_SEX_IDENT_CD').alias("GENDER"),
    col('BENE_RACE_CD').alias('RACE'),
    col('SP_STATE_CODE').alias('STATE'),
    col('BENRES_IP').alias("ANNUAL_COST"),
)

inp = inpatient_1.groupBy('DESYNPUF_ID').agg(
    count("ICD9_DGNS_CD_1").alias("DX"),
    count('ICD9_PRCDR_CD_1').alias('PX'),
    count("HCPCS_CD_1").alias("HCPCS"),
    max("CLM_FROM_DT").alias("DATE"),
)

inner_join = ben.join(inp, ben.PATIENT_ID == inp.DESYNPUF_ID, how='inner')

timeDiff = (unix_timestamp('DATE', "yyyy-MM-dd HH:mm:ss") - unix_timestamp('BENE_BIRTH_DT', "yyyy-MM-dd HH:mm:ss"))
inner_join = inner_join.withColumn("AGE_YRS", timeDiff/60/60/24/365)
df = inner_join.select('ANNUAL_COST','PATIENT_ID','AGE_YRS','GENDER','RACE','STATE','DX','PX','HCPCS')
df.filter(col("ANNUAL_COST")!=0).show()


cat_cols = ['GENDER','RACE','STATE']
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
    'HCPCS'
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

predictions=trained_synpuf_model.transform(unlabeled_data) 
predictions.show()

test_df = test_data.toPandas()
pred_df = predictions.toPandas()


test_df.merge(pred_df).to_csv("synpuf_ml_output.csv")
