#!/bin/bash

# reads a set of fastq files created by CASAVA 1.8+ into HDFS

set -e
set -u

# file names are like:
# LC_33_NoIndex_L001_R1_010.fastq.gz

DATA_DIR=$1
PREFIX=$2
HDFS_DATA_DIR=$3

CLOUDBREAK_HOME=/l2/users/whelanch/gene_rearrange/svpipeline

NUM_FILES=`ls $DATA_DIR/${PREFIX}_R1*.fastq.gz | wc -l`
echo "processing $NUM_FILES file pairs"

for i in $(seq 1 $NUM_FILES)
do
    FILENUM=`printf "%03d" $i`
    hadoop jar ${CLOUDBREAK_HOME}/lib/cloudbreak-1.0-SNAPSHOT-exe.jar readPairedEndFilesIntoHDFS \
	--HDFSDataDir $HDFS_DATA_DIR \
	--fastqFile1 $DATA_DIR/${PREFIX}_R1_${FILENUM}.fastq.gz \
	--fastqFile2 $DATA_DIR/${PREFIX}_R2_${FILENUM}.fastq.gz \
	--outFileName ${PREFIX}_${FILENUM}.txt
done
