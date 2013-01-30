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

# The chromsome length index of your genome reference, on your local filesystem,
# created by running 'samtools faidx reference.fasta'
LOCAL_GENOME_INDEX_FAI=/g/whelanch/indices/human_b36_male_chr2.fasta.fai

###############################################################################
# HDFS FILES AND DIRECTORIES
###############################################################################

# Directory in HDFS to do the work in
HDFS_EXPERIMENT_DIR=/user/whelanch/cloudbreak/venter_chr2_100bp_dip

# Path to the razerS 3 executable in HDFS
HDFS_RAZERS3_EXECUTABLE=/user/whelanch/executables/razers3

# Paths to the FASTA and FAI files for the reference
HDFS_GENOME_INDEX=/user/whelanch/indices/human_b36_male_chr2.fasta
HDFS_GENOME_INDEX_FAI=/user/whelanch/indices/human_b36_male_chr2.fasta.fai

###############################################################################
# Razers3 ALIGNMENT PARAMETERS
###############################################################################
NUM_REPORTS=1000
PCT_IDENTITY=94
SENSITIVITY=99

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
LR_THRESHOLD=1.68
MEDIAN_FILTER_WINDOW=5

# experiment name
NAME=cloudbreak_${LIBRARY_NAME}_${READ_GROUP_NAME}

# read paired end files into HDFS
echo "reading fastq files into HDFS"
time hadoop jar $CLOUDBREAK_HOME/lib/cloudbreak-${project.version}-exe.jar readPairedEndFilesIntoHDFS \
    --HDFSDataDir $HDFS_EXPERIMENT_DIR/data/ \
    --fastqFile1  $FASTQ_FILE_1 \
    --fastqFile2  $FASTQ_FILE_2

# run Razers 3 alignments
echo "Running Razers 3 Alignments in Hadoop" 
time hadoop jar $CLOUDBREAK_HOME/lib/cloudbreak-${project.version}-exe.jar -Dmapred.reduce.tasks=$ALIGNMENT_REDUCE_TASKS razerS3SingleEnds \
    --HDFSDataDir $HDFS_EXPERIMENT_DIR/data/ \
    --HDFSAlignmentsDir $HDFS_EXPERIMENT_DIR/alignments/ \
    --reference $HDFS_GENOME_INDEX \
    --HDFSPathToRazerS3 $HDFS_RAZERS3_EXECUTABLE \
    --numReports $NUM_REPORTS \
    --pctIdentity $PCT_IDENTITY \
    --sensitivity $SENSITIVITY

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

# export GMM results out of HDFS
echo "exporting gmm results from HDFS"
time hadoop jar $CLOUDBREAK_HOME/lib/cloudbreak-${project.version}-exe.jar exportGMMResults \
    --inputHDFSDir $HDFS_EXPERIMENT_DIR/gmm \
    --faidx $LOCAL_GENOME_INDEX_FAI \
    --resolution $RESOLUTION \
    --outputPrefix $NAME

# extract deletion calls
echo "extracting deletion calls"
time hadoop jar $CLOUDBREAK_HOME/lib/cloudbreak-${project.version}-exe.jar extractPositiveRegionsFromWig \
    --inputWigFile ${NAME}_lrHeterozygous.wig.gz \
    --outputBedFile ${NAME}_dels.bed \
    --name ${NAME}_dels \
    --faidx $LOCAL_GENOME_INDEX_FAI \
    --threshold $LR_THRESHOLD \
    --medianFilterWindow $MEDIAN_FILTER_WINDOW \
    --muFile ${NAME}_mu2.wig.gz \
    --targetIsize $INSERT_SIZE \
    --targetIsizeSD $INSERT_SIZE_SD \
    --extraWigFilesToAverage ${NAME}_w0.wig.gz

# genotype the calls based on avg w0
cat ${NAME}_dels.bed | awk 'NR != 1 {OFS="\t"; print $1,$2,$3,$4,$5,$10,($10 < .2 ? "Homozygous" : "Heterozygous")}' > ${NAME}_dels_genotyped.bed

