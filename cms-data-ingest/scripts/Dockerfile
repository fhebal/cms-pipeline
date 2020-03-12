FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    vim \
    unzip \
    tmux

# Install packages
RUN pip3 install boto3 && \
    pip3 install botocore && \
    pip3 install bs4 && \
    pip3 install numpy && \
    pip3 install pandas && \
    pip3 install pyarrow && \
    pip3 install requests && \
    pip3 install argparse && \
    pip3 install zipfile36 && \
    pip3 install sh

COPY . /processing
WORKDIR processing

RUN apt-get update && \
    apt-get install sudo -y && \
    cd /processing && \ 
    mkdir /Files && \
    rm -r /root/.cache/pip

ADD s3api.py /processing/s3api.py

# Install aws cli and set api keys
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o \
"awscliv2.zip"
RUN cd ~
RUN unzip awscliv2.zip && \
    sudo ./aws/install
 
# AWS configuration
RUN aws configure set aws_access_key_id
RUN aws configure set aws_secret_access_key
RUN aws configure set default.region us-east-1

# Run processing script
RUN python3 cmss3.py


