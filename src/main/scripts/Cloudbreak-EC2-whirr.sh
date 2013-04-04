#!/bin/bash

set -e
set -u

CLOUDBREAK_VERSION=${project.version}

# Update the name location of Cloudbreak
CLOUDBREAK_HOME=/l2/users/whelanch/svpipeline/snapshots/cloudbreak-${CLOUDBREAK_VERSION}

WHIRR_PUBLIC_KEY=/g/whelanch/.ssh/id_rsa_whirr

# Update the paths below for the locations of these files:
FASTQ_FILE1=/l2/users/whelanch/gene_rearrange/data/jcvi/human_b36_male_chr2_venterindels_c15_i100_s30_rl100.read1.fastq.gz
FASTQ_FILE2=/l2/users/whelanch/gene_rearrange/data/jcvi/human_b36_male_chr2_venterindels_c15_i100_s30_rl100.read2.fastq.gz
GEM_MAPPER_EXECUTABLE=/g/whelanch/software/GEM-NM-2/gem-mapper
GEM_2_SAM_EXECUTABLE=/g/whelanch/software/GEM-NM-2/gem-2-sam
REFERENCE_GEM_INDEX=/l2/users/whelanch/genome_refs/10KG/hg18/human_b36_male_chr2.gem
REFERENCE_FAIDX=/l2/users/whelanch/genome_refs/10KG/hg18/human_b36_male_chr2.fasta.fai

# update the name of your bucket on S3
MY_BUCKET_NAME=dev.cwhelan.cloudbreak

INSERT_SIZE=300
INSERT_SIZE_SD=30

# read group and library name of your reads
READ_GROUP_NAME=readgroup1
LIBRARY_NAME=venterchr2100bpdip

###############################################################################
# GEM ALIGNMENT PARAMETERS
###############################################################################
GEM_MAPPER_EDIT_DISTANCE=6
GEM_MAPPER_STRATA=2
NUM_REPORTS=1000
GEM_MAPPER_MAX_PROCESSES_ON_NODE=6

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

FASTQ_FILE1_BASENAME=`basename $FASTQ_FILE1`
FASTQ_FILE2_BASENAME=`basename $FASTQ_FILE2`
REFERENCE_GEM_INDEX_BASENAME=`basename $REFERENCE_GEM_INDEX`
REFERENCE_FAIDX_BASENAME=`basename $REFERENCE_FAIDX`

echo "=================================="
echo "Copying files to S3"
echo "=================================="
echo "File: $CLOUDBREAK_JAR"
hadoop jar $CLOUDBREAK_JAR copyToS3 --S3Bucket $MY_BUCKET_NAME --fileName $CLOUDBREAK_JAR
echo "File: $FASTQ_FILE1"
hadoop jar $CLOUDBREAK_JAR copyToS3 --S3Bucket $MY_BUCKET_NAME --fileName $FASTQ_FILE1
echo "File: $FASTQ_FILE2"
hadoop jar $CLOUDBREAK_JAR copyToS3 --S3Bucket $MY_BUCKET_NAME --fileName $FASTQ_FILE2
echo "File: $GEM_MAPPER_EXECUTABLE"
hadoop jar $CLOUDBREAK_JAR copyToS3 --S3Bucket $MY_BUCKET_NAME --fileName $GEM_MAPPER_EXECUTABLE
echo "File: $GEM_2_SAM_EXECUTABLE"
hadoop jar $CLOUDBREAK_JAR copyToS3 --S3Bucket $MY_BUCKET_NAME --fileName $GEM_2_SAM_EXECUTABLE
echo "File: $REFERENCE_GEM_INDEX"
hadoop jar $CLOUDBREAK_JAR copyToS3 --S3Bucket $MY_BUCKET_NAME --fileName $REFERENCE_GEM_INDEX
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
echo "Prepping reads for Cloudbreak"
echo "=================================="

# execute readPairedEndFiles command on cluster
TASK_TRACKER_INSTANCE1_ADDRESS=`cat ~/.whirr/cloudbreak/instances | grep 'hadoop-tasktracker' | head -1 | cut -f3`
echo "running on $TASK_TRACKER_INSTANCE1_ADDRESS "

ssh -o "StrictHostKeyChecking no" -i $WHIRR_PUBLIC_KEY ${TASK_TRACKER_INSTANCE1_ADDRESS} << EOF
    hadoop fs -copyToLocal /user/cloudbreak/$CLOUDBREAK_JAR_NAME .
    hadoop jar $CLOUDBREAK_JAR_NAME readPairedEndFilesIntoHDFS  \
       --HDFSDataDir /user/cloudbreak/data/   \
       --fastqFile1  /user/cloudbreak/$FASTQ_FILE1_BASENAME \
       --fastqFile2  /user/cloudbreak/$FASTQ_FILE2_BASENAME \
       --filesInHDFS
EOF

echo "=================================="
echo "Running GEM Aligner"
echo "=================================="
hadoop jar $CLOUDBREAK_JAR -Dmapred.reduce.tasks=$ALIGNMENT_REDUCE_TASKS  gemSingleEnds \
    --HDFSPathToGEMMapper /user/cloudbreak/gem-mapper \
    --HDFSPathToGEM2SAM /user/cloudbreak/gem-2-sam \
    --reference /user/cloudbreak/$REFERENCE_GEM_INDEX_BASENAME \
    --HDFSAlignmentsDir /user/cloudbreak/alignments \
    --HDFSDataDir /user/cloudbreak/data \
    --numReports $NUM_REPORTS --editDistance $GEM_MAPPER_EDIT_DISTANCE --strata $GEM_MAPPER_STRATA  \
    --maxProcessesOnNode $GEM_MAPPER_MAX_PROCESSES_ON_NODE

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
