FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    unzip

RUN pip3 install boto3 && \
    pip3 install pyspark && \
    pip3 install findspark && \
    pip3 install configparser && \
    pip3 install numpy && \
    pip3 install pandas

ENV SPARK_HOME=/spark/spark-2.4.5-bin-hadoop2.7
ENV JAVA_HOME=/java/jdk1.8.0_241

COPY . /model
WORKDIR model

# Install pyspark dependencies
RUN apt-get update && \
    apt-get install sudo -y && \
    mkdir /java && \
    mv jdk-8u241-linux-x64.tar.gz /java && \
    cd /java && \
    tar -xzvf jdk-8u241-linux-x64.tar.gz && \
    cd /model && \
    mkdir /spark && \
    mv spark-2.4.5-bin-hadoop2.7.tgz /spark && \
    cd /spark && \
    tar -xzvf spark-2.4.5-bin-hadoop2.7.tgz && \
    rm /java/jdk-8u241-linux-x64.tar.gz && \
    rm /spark/spark-2.4.5-bin-hadoop2.7.tgz && \
    rm -r /root/.cache/pip

# install aws cli and set api keys
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN cd ~
RUN unzip awscliv2.zip && \
    sudo ./aws/install
 
# aws configuration
#RUN aws configure set aws_access_key_id
#RUN aws configure set aws_secret_access_key
#RUN aws configure set default.region us-east-1


RUN python3 pyspark_model.py


