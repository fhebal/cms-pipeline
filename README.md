# Health Care Analytics Pipeline
The purpose of this project is to develop and deploy a scalable system for the data science lifecycle in health care use cases using the Synthethic Public Use file curated and made publically available by the Centers for Medicaid and Medicare Services. Production version is be deployed in a cloud-based architecture (AWS) adhering to design principles and best practices for implementing large-scale systems for data ingestion, processing, storage, and analytics. The pipeline includes:
* An end to end process of gathering, preparing data for ML Modeling.
* Collaboration with the team on business use case, data preparation and analysis.
* Connecting to databases, analyzing the data and deriving valuable insights using Spark.
* Build, train machine learning models and deploy them into production environment.

![Image of ML Pipeline](https://github.com/fhebal/cms-pipeline/blob/master/ml_pipeline.png)

## Project goals (MSDS 436 - Course Objectives)
* Select appropriate infrastructure for large data processing, storage and analytics.
* Process data and architect an end-to-end pipeline in a cloud environment.
* Architect an end-to-end solution to deploy a machine learning model into production.
* Review formats and protocols for application programming interfaces.
* Utilize High Performance Computing for distributed in-memory processing of large data.
* Package, distribute and orchestrate containerized applications.

## Software objectives
* Modular: components are modular/isolated to ensure use-case/stack flexibility
* Automated: maximal steps from ingestion to deployment are automated
* Modern stack: Technology must be modern, useful, and widely adopted

## System Architecture

![Image of Architecture](https://i.ibb.co/rsQyYSj/CMS-Pipeline.png)

* End to end pipeline is fully automated with a near serverless architecture
* Automation is implemented with a series of Amazon Lambda functions that starts with an initial function that is scheduled
to trigger on regular intervals using Amazon CloudWatch. Once the scheduled function triggers, a sequence of functions trigger,
which runs the end to end pipeline from data ingestion, machine learning, and loading to Amazon Redshift.
* Each script used for data ingestion, processing, loading, and machine learning are containerized within Docker images.
* EC2 instances containing Docker images are set to start and stop through triggered Lambda functions for cost efficiency.

## Data Ingestion, Analysis, and Preparation
* Amazon Lambda functions were used for automation.
* CMS data was scraped using Python and Beautiful Soup.
* All files include 5 table schemas and 135,666,373 patient records ingested.
* Amazon S3 was the first stopping point for ingested data.
* CSV files converted to Parquet files to compress size and speed up read.
* Webscraping, data ingestion, and transformation script was containerized using Docker and deployed in Amazon EC2 instance. 
* PySpark jobs aggregated data and applied transformations.
* ML script was built in Jupyter and reduced to a scalable Python script.
* ML script was containerized using Docker and deployed in Amazon EC2 Instance.
* Feature engineering, model training and evaluation conducted in container scripts.
* Analysis results output to Parquet file in Amazon Redshift.
* Parquet files in S3 were loaded into Amazon Redshift using Lambda functions that utilized scripts written in Node.js  


## Technology and Tools Used
* Python & Beautiful Soup - Web Scraping
* PySpark - Data Ingestion and Transformation
* Boto3 - Interface with Amazon APIs
* AWS API
* Amazon CLI
* AWS Lambda - Automation/Scheduling Scripts and Batch Processing
* Paramiko - SSHv2 Protocol to Connect to EC2, Open Shell, and Run Docker Image 
* AWS  S3 - Storage
* Spark ML -  Linear Regression
* Docker - Containerization of ETL Process, Feature Engineering, Model Training, and Deploy
* Node.js Load Data into Redshift
* Redshift - Data Warehouse and Insight Result Storage for Deployment to Tableau
* Tableau - Data Visualization and Insight Reporting

## Files

### Processing and Machine Learning Files

#### Data Ingest and Processing:
* cmss3.py - webscrapes CMS website, processes and converts data into Parquet files, and uploads to Amazon S3
* s3api.py - script to connect to Amazon S3 using Boto3

#### Machine Learning:
* pyspark_model.py - using Spark and Spark ML, Parquet files are read to Spark dataframes with PySpark pipeline
functions to transform to appropriate format and datatypes for machine learning. Transformed data is randomly split 0.70 and 0.30
to train and test data. Linear regression model is trained and predictions are made. Data is then uploaded into S3 as a Parquet file.

#### Docker:
* Dockerfile - used to build Docker image.

### Files for Lambda Functions

#### Start and Stop EC2 Instances:
* start-cmss3-ec2.zip - starts EC2 instance for webscraping, processing, and ingestion to S3
* end-cmss3-ec2.zip  - stops EC2 instance after
* start-sparkml-ec2.zip - starts EC2 instance for machine learning and uploading output file to S3
* end-sparkml-ec2.zip - stops EC2 instance after

#### SSH Connect and Run Docker Image:
* lambda-process-cmss3.zip - connects via SSH, opens shell, and runs cmss3.py in Docker image using the Paramiko Library 
* lambda-sparkml.zip - connects via SSH, opens shell, and runs pyspark_model.py in Docker image using the Paramiko Library 

#### Redshift Loader:
* redshift-import.zip - loads data from files in S3 bucket to Amazon Redshift using Node.js 

## Redshift 
* File to create schema and tables, and upload data - DDL_DML.txt
* File for commands to load data for Lambda functions - lambda-redshift-loader-commands.txt

### Create Table:
--Beneficiary Summary--
create table cms.beneficiary_summary (DESYNPUF_ID varchar, BENE_BIRTH_DT date, BENE_DEATH_DT date, BENE_SEX_IDENT_CD int2,
       BENE_RACE_CD int2, BENE_ESRD_IND varchar, SP_STATE_CODE int2, BENE_COUNTY_CD int2,
       BENE_HI_CVRAGE_TOT_MONS int2, BENE_SMI_CVRAGE_TOT_MONS int2,
       BENE_HMO_CVRAGE_TOT_MONS int2, PLAN_CVRG_MOS_NUM int2, SP_ALZHDMTA int2,
       SP_CHF int2, SP_CHRNKIDN int2, SP_CNCR int2, SP_COPD int2, SP_DEPRESSN int2,
       SP_DIABETES int2, SP_ISCHMCHT int2, SP_OSTEOPRS int2, SP_RA_OA int2, SP_STRKETIA int2,
       MEDREIMB_IP float, BENRES_IP float, PPPYMT_IP float, MEDREIMB_OP float, BENRES_OP float,
       PPPYMT_OP float, MEDREIMB_CAR float, BENRES_CAR float, PPPYMT_CAR float);


### Copy Command:
--Beneficiary Summary--
copy cms.beneficiary_summary
from 's3://cms-data-1/Beneficiary_Summary/'
IAM_ROLE 'arn:aws:iam::799687528413:role/RedshiftDWS3FullAccess'
FORMAT AS PARQUET;

### Copy Command for Lambda function:
--Beneficiary Summary--
set search_path to cms; 
begin; 
delete from cms.beneficiary_summary; 
copy cms.beneficiary_summary 
from 's3://cms-data-1/Beneficiary_Summary/' 
IAM_ROLE 'arn:aws:iam::799687528413:role/RedshiftDWS3FullAccess' 
FORMAT AS PARQUET; 
commit;




  