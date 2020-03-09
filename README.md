# Health Care Analytics Pipeline
The purpose of this project is to develop and deploy a scalable system for the data science lifecycle in health care use cases using the Synthethic Public Use file curated and made publically available by the Centers for Medicaid and Medicare Services. Production version is be deployed in a cloud-based architecture (AWS) adhering to design principles and best practices for implementing large-scale systems for data ingestion, processing, storage, and analytics. The pipeline includes:
*  an end to end process of gathering, preparing data for ML Modeling.
* collaboration with the team on business use case, data preparation and analysis
* Connecting to databases, analyzing the data and deriving valuable insights using Spark
* Build, train machine learning models and deploy them into production environment

## Project goals (MSDS 436 - Course Objectives)
* Select appropriate infrastructure for large data processing, storage and analytics
* Process data and architect an end-to-end pipeline in a cloud environment
* Architect an end-to-end solution to deploy a machine learning model into production
* Review formats and protocols for application programming interfaces
* Utilize High Performance Computing for distributed in-memory processing of large datas
* Package, distribute and orchestrate containerized applications

## Software objectives
* Modular: components are modular/isolated to ensure use-case/stack flexibility
* Automated: maximal steps from ingestion to deployment are automated
* Cloud agnostic: all pipeline components should work in gcp, aws, and azure
* Modern stack: Technology must be modern, useful, and widely adopted
* Robust CI/CD: adheres to cs best practices, jira/github integrated development
* Versioned Data Science lifecycle: catalogue datasets, models, and performance metadata for experimentation

![Image of ML Pipeline](https://github.com/fhebal/cms-pipeline/ml_pipeline.png)

## Design documentation
Ingest:
* Avoid data loss
* Automate ingestion with configurable yaml file
* Data profiling must be implemented to assert expected output
* Robust testing must be implemented to confirm successful execution
* New data should trigger ingestion process, evaluation of differences (pachyderm?)
* Detect changes to incoming data (e.g. format, etc..)

Process:
* Automate processing with configurable yaml file
* Data mapping should be compiled for any structural transformations
* Avoid intermediary structures 

Store:
* Maintain flexible storage options
* Storage should follow use case e.g. json for web, parquet for analytics
* SQL queries should be stored outside code somewhere sensible and easily accessible

Analytics:
* cron job to retrain model

Deploy:
* 
