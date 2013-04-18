#!/bin/bash

set -e
set -u

# to run this script, update the values in the first three sections with the appropriate
# paths and parameters for your data and file system

###############################################################################
## EXPERIMENT DETAILS
###############################################################################

# locations of your FASTQ files in the local filesystem
FASTQ_FILE_1=/g/whelanch/cloudbreak_data/venter_sim/new3_c15_i100_s30_rl100.read1.fastq.gz
FASTQ_FILE_2=/g/whelanch/cloudbreak_data/venter_sim/new3_c15_i100_s30_rl100.read2.fastq.gz

# insert size stats of your library
INSERT_SIZE=300
INSERT_SIZE_SD=30

# read group and library name of your reads
READ_GROUP_NAME=readgroup1
LIBRARY_NAME=venterchr2100bpdip

###############################################################################
## LOCAL FILES AND DIRECTORIES
###############################################################################

# directory created when you uncompressed the cloudbreak binary distribution
CLOUDBREAK_HOME=/g/whelanch/cloudbreak-${project.version}

###############################################################################
# HDFS FILES AND DIRECTORIES
###############################################################################

# Directory in HDFS to do the work in
HDFS_EXPERIMENT_DIR=/user/whelanch/cloudbreak/venter_chr2_100bp_dip

# Path to the GEM executables in HDFS
HDFS_GEM_MAPPER_EXECUTABLE=/user/whelanch/executables/gem-mapper
HDFS_GEM2SAM_EXECUTABLE=/user/whelanch/executables/gem-2-sam

# Path to the GEM index for the reference
HDFS_GENOME_INDEX=/user/whelanch/indices/human_b36_male_chr2.gem

# The chromsome length index of your genome reference
# created by running 'samtools faidx reference.fasta'
HDFS_GENOME_INDEX_FAI=/user/whelanch/indices/human_b36_male_chr2.fasta.fai

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

# read paired end files into HDFS
echo "reading fastq files into HDFS"
time hadoop jar $CLOUDBREAK_HOME/lib/cloudbreak-${project.version}-exe.jar readPairedEndFilesIntoHDFS \
    --HDFSDataDir $HDFS_EXPERIMENT_DIR/data/ \
    --fastqFile1  $FASTQ_FILE_1 \
    --fastqFile2  $FASTQ_FILE_2

# run GEM alignments
echo "Running GEM in Hadoop"
time hadoop jar $CLOUDBREAK_HOME/lib/cloudbreak-${project.version}-exe.jar -Dmapred.reduce.tasks=$ALIGNMENT_REDUCE_TASKS gemSingleEnds \
    --HDFSDataDir $HDFS_EXPERIMENT_DIR/data/ \
    --HDFSAlignmentsDir $HDFS_EXPERIMENT_DIR/alignments/ \
    --reference $HDFS_GENOME_INDEX \
    --HDFSPathToGEMMapper $HDFS_GEM_MAPPER_EXECUTABLE \
    --HDFSPathToGEM2SAM $HDFS_GEM2SAM_EXECUTABLE \
    --editDistance $GEM_MAPPER_EDIT_DISTANCE \
    --strata $GEM_MAPPER_STRATA \
    --numReports $NUM_REPORTS \
    --maxProcessesOnNode $GEM_MAPPER_MAX_PROCESSES_ON_NODE


# write a read group info file and copy into HDFS
echo "creating readgroup file"
echo "$READ_GROUP_NAME	$LIBRARY_NAME	$INSERT_SIZE	$INSERT_SIZE_SD	false	$HDFS_EXPERIMENT_DIR/alignments" >> readGroupInfo.txt
hadoop dfs -copyFromLocal readGroupInfo.txt $HDFS_EXPERIMENT_DIR/readGroupInfo.txt

# run Cloudbreak GMM algorithm
echo "runing cloudbreak GMM algorithm"
time hadoop jar $CLOUDBREAK_HOME/lib/cloudbreak-${project.version}-exe.jar -Dmapred.reduce.tasks=$GMM_REDUCE_TASKS GMMFitSingleEndInsertSizes \
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
time hadoop jar $CLOUDBREAK_HOME/lib/cloudbreak-${project.version}-exe.jar extractDeletionCalls \
    --faidx $HDFS_GENOME_INDEX_FAI \
    --threshold $DELETION_LR_THRESHOLD \
    --medianFilterWindow $DELETION_MEDIAN_FILTER_WINDOW \
    --targetIsize $INSERT_SIZE \
    --targetIsizeSD $INSERT_SIZE_SD \
    --inputHDFSDir $HDFS_EXPERIMENT_DIR/gmm/ \
    --outputHDFSDir $HDFS_EXPERIMENT_DIR/del_calls/

hadoop dfs -cat $HDFS_EXPERIMENT_DIR/del_calls/part* | sort -k1,1 -k2,2n > ${NAME}_dels.bed

# genotype the calls based on avg w0
cat ${NAME}_dels.bed | awk 'NR != 1 {OFS="\t"; print $1,$2,$3,$4,$5,$10,($10 < .2 ? "Homozygous" : "Heterozygous")}' > ${NAME}_deletions_genotyped.bed

echo "extracting insertion calls"
time hadoop jar $CLOUDBREAK_HOME/lib/cloudbreak-${project.version}-exe.jar extractInsertionCalls \
    --faidx $HDFS_GENOME_INDEX_FAI \
    --threshold $INSERTION_LR_THRESHOLD \
    --medianFilterWindow $INSERTION_MEDIAN_FILTER_WINDOW \
    --targetIsize $INSERT_SIZE \
    --targetIsizeSD $INSERT_SIZE_SD \
    --inputHDFSDir $HDFS_EXPERIMENT_DIR/gmm/ \
    --outputHDFSDir $HDFS_EXPERIMENT_DIR/ins_calls/

hadoop dfs -cat $HDFS_EXPERIMENT_DIR/ins_calls/part* | sort -k1,1 -k2,2n > ${NAME}_insertions.bed

# genotype the calls based on avg w0
cat ${NAME}_insertions.bed | awk 'NR != 1 {OFS="\t"; print $1,$2,$3,$4,$5,$10,($10 < .2 ? "Homozygous" : "Heterozygous")}' > ${NAME}_insertions_genotyped.bed
