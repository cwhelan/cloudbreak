#!/bin/bash

## This script processes the simulated data set described in the Cloudbreak manuscript on Amazon EC2.
## Before running this script please edit the file cloudbreak-whirr.properties to include your access
## credentials, and make sure you understand the process of creating a cluster on EC2, and the potential
## charges that can arise from it.

set -e
set -u

CLOUDBREAK_VERSION=${project.version}

# Update the name location of Cloudbreak
CLOUDBREAK_HOME=/l2/users/whelanch/svpipeline/snapshots/cloudbreak-${CLOUDBREAK_VERSION}

# Update the location of your public key
WHIRR_PUBLIC_KEY=/g/whelanch/.ssh/id_rsa_whirr

# update the name of your bucket on S3
MY_BUCKET_NAME=cloudbreak.example

INSERT_SIZE=300
INSERT_SIZE_SD=30

# read group and library name of your reads
READ_GROUP_NAME=readgroup1
LIBRARY_NAME=venterchr2100bpdip

###############################################################################
# BWA ALIGNMENT PARAMETERS
###############################################################################
NUM_EXTRA_REPORTS=0
BWA_MAPPER_MAX_PROCESSES_ON_NODE=6

###############################################################################
# CLOUDBREAK PARAMETERS
###############################################################################
ALIGNMENT_REDUCE_TASKS=30
GMM_REDUCE_TASKS=60
MAX_INSERT=25000
RESOLUTION=25
ALIGNER=sam
MAX_MAPQ_DIFF=6
MIN_CLEAN_COVERAGE=3
DELETION_LR_THRESHOLD=2.29
DELETION_MEDIAN_FILTER_WINDOW=5
INSERTION_LR_THRESHOLD=0.26
INSERTION_MEDIAN_FILTER_WINDOW=5

# experiment name
NAME=cloudbreak_${LIBRARY_NAME}_${READ_GROUP_NAME}

# Calculated filenames and paths
CLOUDBREAK_JAR_NAME=cloudbreak-${CLOUDBREAK_VERSION}.jar
CLOUDBREAK_JAR=$CLOUDBREAK_HOME/$CLOUDBREAK_JAR_NAME

# launch cluster with whirr
echo "=================================="
echo "Launching cluster"
echo "=================================="
hadoop jar $CLOUDBREAK_JAR launchCluster

# update current configuration to use EC2 Cluster
OLD_HADOOP_CONF_DIR=HADOOP_CONF_DIR

export HADOOP_CONF_DIR=~/.whirr/cloudbreak/

echo "=================================="
echo "Starting proxy"
echo "=================================="
~/.whirr/cloudbreak/hadoop-proxy.sh &

echo "=================================="
echo "Copying from S3"
echo "=================================="
hadoop distcp s3n://$MY_BUCKET_NAME/ /user/cloudbreak

HDFS_EXPERIMENT_DIR=/user/cloudbreak

echo "=================================="
echo "Running BWA Aligner"
echo "=================================="
time hadoop jar $CLOUDBREAK_JAR -Dmapred.reduce.tasks=$ALIGNMENT_REDUCE_TASKS bwaPairedEnds \
    --HDFSDataDir $HDFS_EXPERIMENT_DIR/data/ \
    --HDFSAlignmentsDir $HDFS_EXPERIMENT_DIR/alignments/ \
    --referenceBasename $HDFS_EXPERIMENT_DIR/indices/human_b36_male_chr2.fasta \
    --HDFSPathToBWA $HDFS_EXPERIMENT_DIR/executables/bwa \
    --HDFSPathToXA2multi $HDFS_EXPERIMENT_DIR/executables/xa2multi.pl \
    --numExtraReports $NUM_EXTRA_REPORTS \
    --maxProcessesOnNode $BWA_MAPPER_MAX_PROCESSES_ON_NODE

echo "=================================="
echo "Creating a readgroup file"
echo "=================================="
echo "$READ_GROUP_NAME	$LIBRARY_NAME	$INSERT_SIZE	$INSERT_SIZE_SD	false	/user/cloudbreak/alignments" >> readGroupInfo.txt
hadoop dfs -copyFromLocal readGroupInfo.txt $HDFS_EXPERIMENT_DIR/readGroupInfo.txt

echo "=================================="
echo "Running GMM Feature Generation"
echo "=================================="
hadoop jar $CLOUDBREAK_JAR -Dmapred.reduce.tasks=$GMM_REDUCE_TASKS GMMFitSingleEndInsertSizes \
    --inputFileDescriptor $HDFS_EXPERIMENT_DIR/readGroupInfo.txt  \
    --outputHDFSDir $HDFS_EXPERIMENT_DIR/gmm_features/ \
    --faidx $HDFS_EXPERIMENT_DIR/indices/human_b36_male_chr2.fasta.fai \
    --maxInsertSize $MAX_INSERT    --resolution $RESOLUTION --aligner sam    --maxLogMapqDiff $MAX_MAPQ_DIFF --minCleanCoverage $MIN_CLEAN_COVERAGE

echo "=================================="
echo "Extracting Deletion Calls"
echo "=================================="
time hadoop jar $CLOUDBREAK_JAR extractDeletionCalls \
    --faidx $HDFS_EXPERIMENT_DIR/indices/human_b36_male_chr2.fasta.fai  \
    --threshold $DELETION_LR_THRESHOLD \
    --medianFilterWindow $DELETION_MEDIAN_FILTER_WINDOW \
    --targetIsize $INSERT_SIZE \
    --targetIsizeSD $INSERT_SIZE_SD \
    --inputHDFSDir $HDFS_EXPERIMENT_DIR/gmm_features/ \
    --outputHDFSDir $HDFS_EXPERIMENT_DIR/del_calls/

hadoop dfs -cat $HDFS_EXPERIMENT_DIR/del_calls/part* | sort -k1,1 -k2,2n > ${NAME}_dels.bed

# genotype the calls based on avg w0
cat ${NAME}_dels.bed | awk 'NR != 1 {OFS="\t"; print $1,$2,$3,$4,$5,$10,($10 < .2 ? "Homozygous" : "Heterozygous")}' > ${NAME}_deletions_genotyped.bed

echo "=================================="
echo "Extracting Insertion Calls"
echo "=================================="
time hadoop jar $CLOUDBREAK_JAR extractInsertionCalls \
    --faidx $HDFS_EXPERIMENT_DIR/indices/human_b36_male_chr2.fasta.fai \
    --threshold $INSERTION_LR_THRESHOLD \
    --medianFilterWindow $INSERTION_MEDIAN_FILTER_WINDOW \
    --targetIsize $INSERT_SIZE \
    --targetIsizeSD $INSERT_SIZE_SD \
    --inputHDFSDir $HDFS_EXPERIMENT_DIR/gmm_features/ \
    --outputHDFSDir $HDFS_EXPERIMENT_DIR/ins_calls/

hadoop dfs -cat $HDFS_EXPERIMENT_DIR/ins_calls/part* | sort -k1,1 -k2,2n > ${NAME}_insertions.bed

# genotype the calls based on avg w0
cat ${NAME}_insertions.bed | awk 'NR != 1 {OFS="\t"; print $1,$2,$3,$4,$5,$10,($10 < .2 ? "Homozygous" : "Heterozygous")}' > ${NAME}_insertions_genotyped.bed

echo "=================================="
echo "Shutting down the cluster"
echo "=================================="
hadoop jar $CLOUDBREAK_JAR destroyCluster

export HADOOP_CONF_DIR=$OLD_HADOOP_CONF_DIR
