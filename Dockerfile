FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:02ab-main

RUN python3 -m pip install --upgrade latch

# install Trim Galore, Cutadapt, and FastQC
RUN curl -L https://github.com/FelixKrueger/TrimGalore/archive/refs/tags/0.6.10.tar.gz -o trim_galore-0.6.10.tar.gz &&\
    tar -zxf trim_galore-0.6.10.tar.gz &&\
    rm trim_galore-0.6.10.tar.gz
RUN pip3 install cutadapt==4.3
RUN apt update
RUN apt-get -y install default-jdk
RUN curl -L https://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v0.12.1.zip -o fastqc-v0.12.1.zip &&\
    unzip fastqc-v0.12.1.zip &&\
    rm -rf fastqc-v0.12.1.zip 
ENV PATH="$PATH:FastQC/"

WORKDIR /root

COPY wf wf

#create local directory to store output of cmd later
RUN mkdir trim_galore_out/

ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
