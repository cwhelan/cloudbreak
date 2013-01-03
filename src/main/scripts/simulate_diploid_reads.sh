#!/bin/bash

set -e
set -u

HAP1_REF=$1
HAP2_REF=$2
OUTPUT_PREFIX=$3
COV_PER_HAPLOTYPE=$4
ISIZE=$5
ISIZE_SD=$6
READ_LENGTH=$7

OUTNAME=${OUTPUT_PREFIX}_c${COV_PER_HAPLOTYPE}_i${ISIZE}_s${ISIZE_SD}_rl${READ_LENGTH}

# dwgsim params:
#
# -e and -E: 0.02 error rate
# -d, -s, -1, -2: internal isize, std dev, read lengths
# -C: coverage per haplotype
# -r: 0.0010 mutation rate (default)
# -R: no indels (indels already applied to reference)
# -H: haploid mode

dwgsim -e 0.02-0.02 -E 0.02-0.02 -d $ISIZE -s $ISIZE_SD -1 $READ_LENGTH -2 $READ_LENGTH -C $COV_PER_HAPLOTYPE -r 0.0010 -R 0 -H -P hap1 $HAP1_REF $HAP1_REF
dwgsim -e 0.02-0.02 -E 0.02-0.02 -d $ISIZE -s $ISIZE_SD -1 $READ_LENGTH -2 $READ_LENGTH -C $COV_PER_HAPLOTYPE -r 0.0010 -R 0 -H -P hap2 $HAP2_REF $HAP2_REF

cat ${HAP1_REF}.bwa.read1.fastq ${HAP2_REF}.bwa.read1.fastq | gzip -c > ${OUTNAME}.read1.fastq.gz
cat ${HAP1_REF}.bwa.read2.fastq ${HAP2_REF}.bwa.read2.fastq | gzip -c > ${OUTNAME}.read2.fastq.gz

rm ${HAP1_REF}.bwa.read1.fastq
rm ${HAP1_REF}.bwa.read2.fastq
rm ${HAP1_REF}.bfast.fastq

rm ${HAP2_REF}.bwa.read1.fastq
rm ${HAP2_REF}.bwa.read2.fastq
rm ${HAP2_REF}.bfast.fastq
