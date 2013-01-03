#!/bin/bash

set -e
set -u

if ! [ -a ./cloudbreak_params ]
then
    echo "Could not find cloudbreak_params file"
fi

source ./cloudbreak_params

if ! ( [ -n $SAMPLE_NAME ] && [ -n $HDFS_SAMPLE_DIR ] && [ -n $BUILD_DIR ] && [ -n $LOCAL_FAI ] && [ -n $HDFS_FAI ] )
then
	echo cloudbreak_params needs to have:
	echo
	echo SAMPLE_NAME: overall name of the experiment
	echo HDFS_SAMPLE_DIR: HDFS dir to store the score pileup in
	echo BUILD_DIR: location on the local filesystem to build cloudbreak in
	echo GENOME_NAME: genome name for igv: hg18, hg19
	echo LOCAL_FAI: location of the genome .fai on the local filesystem
	echo HDFS_FAI: location of the genome .fai on the HDFS filesystem
fi

READ_GROUP_FILE=$1
MAPABILITY=$2
FILTER=$3
RESOLUTION=$4
MEDIAN_FILTER_WINDOW=$5
ALIGNER=$6
MAX_INSERT=$7
TRUTH=$8
THRESHOLD_MIN=$9
SHORT_NAME=${10}
MAX_MAPQ_DIFF=${11}
MIN_ALIGNMENT_SCORE=${12}
MAX_MISMATCH_FILTER=${13}
MIN_CLEAN_COVERAGE=${14}

pushd $BUILD_DIR
git pull
mvn clean
mvn assembly:assembly
SHORT_GIT_TAG=`git rev-parse --short HEAD`
popd

NAME=${SHORT_NAME}_`basename $READ_GROUP_FILE | awk -F'.' '{print $1}'`_`basename $MAPABILITY | awk -F'.' '{print $1}'`_`basename $FILTER | awk -F'.' '{print $1}'`_${MAX_INSERT}_${RESOLUTION}_${ALIGNER}_${MAX_MAPQ_DIFF}_${SHORT_GIT_TAG}_${MIN_ALIGNMENT_SCORE}_${MAX_MISMATCH_FILTER}_${MIN_CLEAN_COVERAGE}

echo Experiment name: $NAME

mkdir /tmp/$NAME
pushd /tmp/$NAME

exec > >(tee -a test.log)

cat <<EOF
Parameters:

EXPERIMENT=$NAME

SOFTWARE_VERSION=$SHORT_GIT_TAG

SAMPLE_NAME=$SAMPLE_NAME
HDFS_SAMPLE_DIR=$HDFS_SAMPLE_DIR
BUILD_DIR=$BUILD_DIR
LOCAL_FAI=$LOCAL_FAI
HDFS_FAI=$HDFS_FAI
READ_GROUP_FILE=$READ_GROUP_FILE
MAPABILITY=$MAPABILITY
FILTER=$FILTER
RESOLUTION=$RESOLUTION
MEDIAN_FILTER_WINDOW=$MEDIAN_FILTER_WINDOW
ALIGNER=$ALIGNER
MAX_INSERT=$MAX_INSERT
TRUTH=$TRUTH
THRESHOLD_MIN=$THRESHOLD_MIN
SHORT_NAME=$SHORT_NAME
MAX_MAPQ_DIFF=$MAX_MAPQ_DIFF
MIN_ALIGNMENT_SCORE=$MIN_ALIGNMENT_SCORE

EOF

MAPABILITY_PARAM=""
if [ $MAPABILITY != "None" ]
then
MAPABILITY_PARAM="--mapabilityWeighting $MAPABILITY"
fi

FILTER_PARAM=""
if [ $FILTER != "None" ]
then
FILTER_PARAM="--excludePairsMappingIn $FILTER"
fi

MIN_ALIGN_SCORE_PARAM=""
if [ $MIN_ALIGNMENT_SCORE != "None" ]
then
MIN_ALIGN_SCORE_PARAM="--minScore $MIN_ALIGNMENT_SCORE"
fi


cat <<EOF

hadoop jar $BUILD_DIR/target/cloudbreak-1.0-SNAPSHOT-exe.jar -Dmapred.reduce.tasks=200 GMMFitSingleEndInsertSizes
   --inputFileDescriptor $READ_GROUP_FILE 
   --outputHDFSDir $HDFS_SAMPLE_DIR/$NAME 
   --faidx $HDFS_FAI 
   --maxInsertSize $MAX_INSERT 
   $MAPABILITY_PARAM
   $FILTER_PARAM
   --resolution $RESOLUTION 
   --aligner $ALIGNER
   --maxLogMapqDiff $MAX_MAPQ_DIFF
   $MIN_ALIGN_SCORE_PARAM
   --maxMismatchFilter $MAX_MISMATCH_FILTER
   --minCleanCoverage $MIN_CLEAN_COVERAGE

EOF

hadoop jar $BUILD_DIR/target/cloudbreak-1.0-SNAPSHOT-exe.jar -Dmapred.reduce.tasks=200 GMMFitSingleEndInsertSizes \
    --inputFileDescriptor $READ_GROUP_FILE \
    --outputHDFSDir $HDFS_SAMPLE_DIR/$NAME \
    --faidx $HDFS_FAI \
    --maxInsertSize $MAX_INSERT \
    $MAPABILITY_PARAM \
    $FILTER_PARAM \
    --resolution $RESOLUTION \
    --aligner $ALIGNER \
    --maxLogMapqDiff $MAX_MAPQ_DIFF \
    --minScore $MIN_ALIGNMENT_SCORE \
    --maxMismatches $MAX_MISMATCH_FILTER \
    --minCleanCoverage $MIN_CLEAN_COVERAGE

cat <<EOF

hadoop jar $BUILD_DIR/target/cloudbreak-1.0-SNAPSHOT-exe.jar sortGMMResults \
    --inputHDFSDir $HDFS_SAMPLE_DIR/$NAME
    --outputHDFSDir $HDFS_SAMPLE_DIR/${NAME}_mergesort

EOF

hadoop jar $BUILD_DIR/target/cloudbreak-1.0-SNAPSHOT-exe.jar sortGMMResults \
    --inputHDFSDir $HDFS_SAMPLE_DIR/$NAME \
    --outputHDFSDir $HDFS_SAMPLE_DIR/${NAME}_mergesort

cat <<EOF

hadoop jar $BUILD_DIR/target/cloudbreak-1.0-SNAPSHOT-exe.jar exportGMMResults \
    --inputHDFSDir $HDFS_SAMPLE_DIR/${NAME}_mergesort \
    --faidx $LOCAL_FAI \
    --resolution $RESOLUTION --outputPrefix $NAME

EOF

hadoop jar $BUILD_DIR/target/cloudbreak-1.0-SNAPSHOT-exe.jar exportGMMResults \
    --inputHDFSDir $HDFS_SAMPLE_DIR/${NAME}_mergesort \
    --faidx $LOCAL_FAI \
    --resolution $RESOLUTION --outputPrefix $NAME

for f in *.wig.gz
do
    ~/software/IGVTools/igvtools tile $f $f.tdf $GENOME_NAME &
done

popd

cp /tmp/${NAME}/${NAME}_lrHeterozygous.wig.gz .
cp /tmp/${NAME}/${NAME}_mu2.wig.gz .

cat <<EOF

python ../build/svpipeline/src/main/scripts/evalWigFile.py \
    ${NAME}_lrHeterozygous.wig.gz $TRUTH $LOCAL_FAI $MEDIAN_FILTER_WINDOW 0.5 ${NAME}_mu2.wig.gz > ${NAME}_lrHeterozygous.perf.txt

EOF

python ../build/svpipeline/src/main/scripts/evalWigFile.py \
    ${NAME}_lrHeterozygous.wig.gz $TRUTH $LOCAL_FAI $MEDIAN_FILTER_WINDOW 0.5 ${NAME}_mu2.wig.gz > ${NAME}_lrHeterozygous.perf.txt
