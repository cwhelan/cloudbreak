#!/bin/bash

# reads a set of fastq files created by CASAVA 1.8+ into HDFS

set -e
set -u

# file names are like:
# LC_33_NoIndex_L001_R1_010.fastq.gz

DATA_DIR=$1
PREFIX=$2
HDFS_DATA_DIR=$3
CLOUDBREAK_HOME=$4

NUM_FILES=`ls $DATA_DIR/${PREFIX}_*.fastq.gz | wc -l`

for i in 1 .. $NUM_FILES
do
    FILENUM=`printf "%03d" $i`
echo    hadoop jar ${CLOUDBREAK_HOME}/lib/cloudbreak-${project.version}-exe.jar readPairedEndFilesIntoHDFS \
echo	--HDFSDataDir $HDFS_DATA_DIR \
echo	--fastqFile1 $DATA_DIR/${PREFIX}_R1_${FILENUM}.fastq.gz \
echo	--fastqFile2 $DATA_DIR/${PREFIX}_R2_${FILENUM}.fastq.gz \
echo	--outFileName ${PREFIX}_${FILENUM}.txt
echo    --filterBasedOnCasava18Flags true

hadoop jar ${CLOUDBREAK_HOME}/lib/cloudbreak-${project.version}-exe.jar readPairedEndFilesIntoHDFS \
	--HDFSDataDir $HDFS_DATA_DIR \
	--fastqFile1 $DATA_DIR/${PREFIX}_R1_${FILENUM}.fastq.gz \
	--fastqFile2 $DATA_DIR/${PREFIX}_R2_${FILENUM}.fastq.gz \
	--outFileName ${PREFIX}_${FILENUM}.txt
    --filterBasedOnCasava18Flags true

done
