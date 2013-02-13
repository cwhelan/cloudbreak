#!/usr/bin/env python

import sys
import subprocess
import evalBedFile

# for deletions:
#   expects bed input - chr, start, stop, and the S1 score from Pindel
#   created by grepping the pindel_D file for the header lines
#   eg
#   cat human_b36_male_chr2_venterindels_c15_i100_s30_rl100_sort_D | awk '$2 == "D" && $14 - $13 > 10 {OFS="\t"; print $8,$13,$14,$25}' > human_b36_male_chr2_venterindels_c15_i100_s30_rl100_sort_D.bed
# for insertions:
#

pindel_filename = sys.argv[1]
truth_filename = sys.argv[2]
sv_type = sys.argv[3]

score_values = []

pindel_file = open(pindel_filename, "r")
for line in pindel_file:
    fields = line.split("\t")
    score = float(fields[3])
    score_values.append(score)

pindel_file.close()

unique_score_values = list(set(score_values))
unique_score_values.sort()

print "\t".join(["Thresh", "Calls", "TP", "WrongType", "Short", "TPR"])
for v in unique_score_values:
    calls_gte_threshold = []
    pindel_file = open(pindel_filename, "r")
    non_del_calls = 0
    for line in pindel_file:
        fields = line.split("\t")
        if float(fields[3]) >= v:
            chrom = fields[0]
            ostart = fields[1]
            oend = fields[2]
            if (sv_type == "DEL"):
                bed_line = "\t".join([chrom, ostart, oend])
            else:
                bed_line = "\t".join([chrom, ostart, oend, fields[4]])
            #print bed_line.strip()
            calls_gte_threshold.append(bed_line)
    if sv_type == "DEL":
        (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_deletions(truth_filename, calls_gte_threshold)
    else:
        (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_insertions(truth_filename, calls_gte_threshold)
    if (qualified_calls > 0):
        tpr = float(matches) / (qualified_calls)
    else:
        tpr = "NA"
    print "\t".join(map(str, [v, qualified_calls, matches, non_del_calls, short_calls, tpr]))
    
    
