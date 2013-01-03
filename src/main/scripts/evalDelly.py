#!/usr/bin/env python

import sys
import subprocess
import evalBedFile

# Delly file format (when only del summaries in file - cat *.del.txt | grep Deletion)
# The summary line contains the chromosome, the estimated start and end of the structural variant,
# the size of the variant, the number of supporting pairs, the average mapping quality and a unique structural variant id.
# 2       3666033 3666250 217     2       1.5     >Deletion_JCVICHR2SIM_00000053<

delly_filename = sys.argv[1]
truth_filename = sys.argv[2]

score_values = []

delly_file = open(delly_filename, "r")
for line in delly_file:
    if line.startswith("#"):
        continue
    fields = line.split("\t")

    # use num pairs as score for now
    score = float(fields[4])
    score_values.append(score)

delly_file.close()

unique_score_values = list(set(score_values))
unique_score_values.sort()

print "\t".join(["Thresh", "Calls", "TP", "WrongType", "Short", "TPR"])
for v in unique_score_values:
    calls_gte_threshold = []
    delly_file = open(delly_filename, "r")
    non_del_calls = 0
    for line in delly_file:
        if line.startswith("#"):
            continue
        fields = line.split("\t")
        if float(fields[4]) >= v:
            chrom = fields[0]
            ostart = fields[1]
            oend = fields[2]
            bed_line = "\t".join([chrom, ostart, oend])
            #print bed_line.strip()
            calls_gte_threshold.append(bed_line)


    (qualified_calls, matches, short_calls) = evalBedFile.eval_bed(truth_filename, calls_gte_threshold)
    tpr = float(matches) / (qualified_calls)
    print "\t".join(map(str, [v, qualified_calls, matches, non_del_calls, short_calls, tpr]))

    
