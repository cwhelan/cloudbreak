#!/bin/bash

set -e
set -u

CLOUDBREAK_VERSION=${project.version}

# Update the name location of Cloudbreak
CLOUDBREAK_HOME=/l2/users/whelanch/svpipeline/snapshots/cloudbreak-${CLOUDBREAK_VERSION}

# Update the location of your public key
WHIRR_PUBLIC_KEY=/g/whelanch/.ssh/id_rsa_whirr

# Update the paths below for the locations of these files:
BAM_FILE=/l2/users/whelanch/gene_rearrange/data/jcvi/human_b36_male_chr2_venterindels_c15_i100_s30_rl100.bam

REFERENCE_FAIDX=/g/whelanch/genome_refs/10KG/hg18/human_b36_male_chr2.fasta.fai

# update the name of your bucket on S3
MY_BUCKET_NAME=dev.cwhelan.cloudbreak

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
ALIGNMENT_REDUCE_TASKS=50
GMM_REDUCE_TASKS=100
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

BAM_FILE_BASENAME=`basename $BAM_FILE`
REFERENCE_FAIDX_BASENAME=`basename $REFERENCE_FAIDX`

echo "=================================="
echo "Copying files to S3"
echo "=================================="
echo "File: $CLOUDBREAK_JAR"
hadoop jar $CLOUDBREAK_JAR copyToS3 --S3Bucket $MY_BUCKET_NAME --fileName $CLOUDBREAK_JAR
echo "File: $REFERENCE_FAIDX"
hadoop jar $CLOUDBREAK_JAR copyToS3 --S3Bucket $MY_BUCKET_NAME --fileName $REFERENCE_FAIDX

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

echo "=================================="
echo "Reading alignments into HDFS"
echo "=================================="
time hadoop jar $CLOUDBREAK_HOME/cloudbreak-${project.version}.jar readSAMFileIntoHDFS \
    --HDFSDataDir $HDFS_EXPERIMENT_DIR/alignments/ \
    --samFile  $BAM_FILE


echo "=================================="
echo "Creating a readgroup file"
echo "=================================="
echo "creating readgroup file"
echo "$READ_GROUP_NAME	$LIBRARY_NAME	$INSERT_SIZE	$INSERT_SIZE_SD	false	/user/cloudbreak/alignments" >> readGroupInfo.txt
hadoop dfs -copyFromLocal readGroupInfo.txt /user/cloudbreak/readGroupInfo.txt

hadoop jar $CLOUDBREAK_JAR -Dmapred.reduce.tasks=$GMM_REDUCE_TASKS GMMFitSingleEndInsertSizes \
    --inputFileDescriptor /user/cloudbreak/readGroupInfo.txt  \
    --outputHDFSDir /user/cloudbreak/gmm_features/ \
    --faidx /user/cloudbreak/$REFERENCE_FAIDX_BASENAME \
    --maxInsertSize $MAX_INSERT    --resolution $RESOLUTION --aligner sam    --maxLogMapqDiff $MAX_MAPQ_DIFF --minCleanCoverage $MIN_CLEAN_COVERAGE

echo "=================================="
echo "Extracting Deletion Calls"
echo "=================================="
time hadoop jar $CLOUDBREAK_JAR extractDeletionCalls \
    --faidx /user/cloudbreak/$REFERENCE_FAIDX_BASENAME  \
    --threshold $DELETION_LR_THRESHOLD \
    --medianFilterWindow $DELETION_MEDIAN_FILTER_WINDOW \
    --targetIsize $INSERT_SIZE \
    --targetIsizeSD $INSERT_SIZE_SD \
    --inputHDFSDir /user/cloudbreak/gmm_features/ \
    --outputHDFSDir /user/cloudbreak/del_calls/

hadoop dfs -cat /user/cloudbreak/del_calls/part* | sort -k1,1 -k2,2n > ${NAME}_dels.bed

# genotype the calls based on avg w0
cat ${NAME}_dels.bed | awk 'NR != 1 {OFS="\t"; print $1,$2,$3,$4,$5,$10,($10 < .2 ? "Homozygous" : "Heterozygous")}' > ${NAME}_deletions_genotyped.bed

echo "=================================="
echo "Extracting Insertion Calls"
echo "=================================="
time hadoop jar $CLOUDBREAK_JAR extractInsertionCalls \
    --faidx /user/cloudbreak/$REFERENCE_FAIDX_BASENAME \
    --threshold $INSERTION_LR_THRESHOLD \
    --medianFilterWindow $INSERTION_MEDIAN_FILTER_WINDOW \
    --targetIsize $INSERT_SIZE \
    --targetIsizeSD $INSERT_SIZE_SD \
    --inputHDFSDir /user/cloudbreak/gmm_features/ \
    --outputHDFSDir /user/cloudbreak/ins_calls/

hadoop dfs -cat /user/cloudbreak/ins_calls/part* | sort -k1,1 -k2,2n > ${NAME}_insertions.bed

# genotype the calls based on avg w0
cat ${NAME}_insertions.bed | awk 'NR != 1 {OFS="\t"; print $1,$2,$3,$4,$5,$10,($10 < .2 ? "Homozygous" : "Heterozygous")}' > ${NAME}_insertions_genotyped.bed

echo "=================================="
echo "Shutting down the cluster"
echo "=================================="
hadoop jar $CLOUDBREAK_JAR destroyCluster

export HADOOP_CONF_DIR=$OLD_HADOOP_CONF_DIR
