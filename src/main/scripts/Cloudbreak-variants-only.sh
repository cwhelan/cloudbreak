#!/bin/bash

set -e
set -u

# to run this script, update the values in the first three sections with the appropriate
# paths and parameters for your data and file system

###############################################################################
## EXPERIMENT DETAILS
###############################################################################

# locations of your name-sorted BAM file in the local filesystem
BAM_FILE=/g/whelanch/cloudbreak_data/venter_sim/new3_c15_i100_s30_rl100.name_sort.bam

# insert size stats of your library
INSERT_SIZE=300
INSERT_SIZE_SD=30

# read group and library name of your reads
READ_GROUP_NAME=readgroup1
LIBRARY_NAME=venterchr2100bpdip

###############################################################################
## LOCAL FILES AND DIRECTORIES
###############################################################################

# directory where the cloudbreak-${project.version}.jar file is
CLOUDBREAK_HOME=/g/whelanch/cloudbreak-${project.version}

###############################################################################
# HDFS FILES AND DIRECTORIES
###############################################################################

# Directory in HDFS to do the work in
HDFS_EXPERIMENT_DIR=/user/whelanch/cloudbreak/venter_chr2_100bp_dip

# The chromsome length index of your genome reference
# created by running 'samtools faidx reference.fasta'
HDFS_GENOME_INDEX_FAI=/user/whelanch/indices/human_b36_male_chr2.fasta.fai

###############################################################################
# CLOUDBREAK PARAMETERS
###############################################################################
GMM_REDUCE_TASKS=100
MAX_INSERT=25000
RESOLUTION=25
ALIGNER=sam
MAX_MAPQ_DIFF=6
MIN_CLEAN_COVERAGE=3
DELETION_LR_THRESHOLD=1.98
DELETION_MEDIAN_FILTER_WINDOW=5
INSERTION_LR_THRESHOLD=0.26
INSERTION_MEDIAN_FILTER_WINDOW=5
GENOTYPING_ALPHA_THRESHOLD=.35

# experiment name
NAME=cloudbreak_${LIBRARY_NAME}_${READ_GROUP_NAME}

echo "reading BAM file into HDFS"
time HADOOP_HEAP_SIZE=4000 hadoop jar $CLOUDBREAK_HOME/cloudbreak-${project.version}.jar readSAMFileIntoHDFS \
    --HDFSDataDir $HDFS_EXPERIMENT_DIR/alignments_import/ \
    --samFile  $BAM_FILE

echo "preparing the reads for Cloudbreak"
time hadoop jar $CLOUDBREAK_HOME/cloudbreak-${project.version}.jar -Dmapred.reduce.tasks=25 prepSAMRecords \
    --inputHDFSDir $HDFS_EXPERIMENT_DIR/alignments_import/ \
    --outputHDFSDir $HDFS_EXPERIMENT_DIR/alignments/

# write a read group info file and copy into HDFS
echo "creating readgroup file"
echo "$READ_GROUP_NAME	$LIBRARY_NAME	$INSERT_SIZE	$INSERT_SIZE_SD	false	$HDFS_EXPERIMENT_DIR/alignments" >> readGroupInfo.txt
hadoop dfs -copyFromLocal readGroupInfo.txt $HDFS_EXPERIMENT_DIR/readGroupInfo.txt

# run Cloudbreak GMM algorithm
echo "runing cloudbreak GMM algorithm"
time hadoop jar $CLOUDBREAK_HOME/cloudbreak-${project.version}.jar -Dmapred.reduce.tasks=$GMM_REDUCE_TASKS GMMFitSingleEndInsertSizes \
   --inputFileDescriptor $HDFS_EXPERIMENT_DIR/readGroupInfo.txt \
   --outputHDFSDir $HDFS_EXPERIMENT_DIR/gmm/ \
   --faidx $HDFS_GENOME_INDEX_FAI \
   --maxInsertSize $MAX_INSERT \
   --resolution $RESOLUTION \
   --aligner $ALIGNER \
   --maxLogMapqDiff $MAX_MAPQ_DIFF \
   --minCleanCoverage $MIN_CLEAN_COVERAGE \

# extract deletion calls
echo "extracting deletion calls"
time hadoop jar $CLOUDBREAK_HOME/cloudbreak-${project.version}.jar extractDeletionCalls \
    --faidx $HDFS_GENOME_INDEX_FAI \
    --threshold $DELETION_LR_THRESHOLD \
    --medianFilterWindow $DELETION_MEDIAN_FILTER_WINDOW \
    --targetIsize $INSERT_SIZE \
    --targetIsizeSD $INSERT_SIZE_SD \
    --inputHDFSDir $HDFS_EXPERIMENT_DIR/gmm/ \
    --outputHDFSDir $HDFS_EXPERIMENT_DIR/del_calls/

hadoop dfs -cat $HDFS_EXPERIMENT_DIR/del_calls/part* | sort -k1,1 -k2,2n > ${NAME}_dels.bed

# genotype the calls based on avg w0
cat ${NAME}_dels.bed | awk 'NR != 1 {OFS="\t"; print $1,$2,$3,$4,$5,$10,($10 < $GENOTYPING_ALPHA_THRESHOLD ? "Homozygous" : "Heterozygous")}' > ${NAME}_deletions_genotyped.bed

echo "extracting insertion calls"
time hadoop jar $CLOUDBREAK_HOME/cloudbreak-${project.version}.jar extractInsertionCalls \
    --faidx $HDFS_GENOME_INDEX_FAI \
    --threshold $INSERTION_LR_THRESHOLD \
    --medianFilterWindow $INSERTION_MEDIAN_FILTER_WINDOW \
    --targetIsize $INSERT_SIZE \
    --targetIsizeSD $INSERT_SIZE_SD \
    --inputHDFSDir $HDFS_EXPERIMENT_DIR/gmm/ \
    --outputHDFSDir $HDFS_EXPERIMENT_DIR/ins_calls/ \
    --noCovFilter

hadoop dfs -cat $HDFS_EXPERIMENT_DIR/ins_calls/part* | sort -k1,1 -k2,2n > ${NAME}_insertions.bed

# genotype the calls based on avg w0
cat ${NAME}_insertions.bed | awk 'NR != 1 {OFS="\t"; print $1,$2,$3,$4,$5,$10,($10 < $GENOTYPING_ALPHA_THRESHOLD ? "Homozygous" : "Heterozygous")}' > ${NAME}_insertions_genotyped.bed
