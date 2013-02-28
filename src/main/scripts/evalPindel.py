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

sv_type = sys.argv[1]
if not (sv_type == "INS" or sv_type == "DEL"):
    print "SV_TYPE needs to be INS or DEL, got " + sv_type
    sys.exit(0)

truth_filename = sys.argv[2]
pindel_deletions_filename = "NA"
pindel_short_insertions_filename = "NA"
pindel_long_insertions_filename = "NA"
if sv_type == "DEL":
    pindel_deletions_filename = sys.argv[3]
    options_pos = 4
else:
    pindel_short_insertions_file = sys.argv[3]
    pindel_long_insertions_file = sys.argv[4]
    options_pos = 5


score_values = []

print_hits = False
short_threshold = 40

if len(sys.argv) > options_pos and sys.argv[options_pos] == "--printHits":
    threshold = float(sys.argv[options_pos + 1])
    score_values.append(threshold)
    print_hits = True
else:
    if sv_type == "DEL":
        pindel_file = open(pindel_deletions_filename, "r")
        for line in pindel_file:
            fields = line.split()
            if len(fields) < 2 or fields[1] != "D":
                continue
            if int(fields[13]) < short_threshold:
                continue
            score = float(fields[24])
            score_values.append(score)
        pindel_file.close()
    else:
        pindel_file = open(pindel_short_insertions_file, "r")
        for line in pindel_file:
            fields = line.split()
            if len(fields) < 2 or fields[1] != "I":
                continue
            if int(fields[2]) <= short_threshold:
                continue
            score = float(fields[24])
            score_values.append(score)
        pindel_file.close()
        pindel_file = open(pindel_long_insertions_file, "r")
        for line in pindel_file:
            fields = line.split()
            if len(fields) < 2 or fields[1] != "LI":
                continue
            score = float(fields[6]) * float(fields(9))
            score_values.append(score)
        pindel_file.close()


unique_score_values = list(set(score_values))
unique_score_values.sort()
print unique_score_values

if not print_hits:
    print "\t".join(["Thresh", "Calls", "TP", "WrongType", "Short", "TPR"])
for v in unique_score_values:
    calls_gte_threshold = []
    if (sv_type == "DEL"):
        pindel_file = open(pindel_deletions_filename, "r")
        for line in pindel_file:
            fields = line.split()
            if len(fields) < 2 or fields[1] != "D":
                continue
            if int(fields[2]) < short_threshold:
                continue
            if float(fields[24]) >= v:
                chrom = fields[7]
                ostart = fields[12]
                oend = fields[13]
                bed_line = "\t".join([chrom, ostart, oend])
                #print bed_line.strip()
                calls_gte_threshold.append(bed_line)
        pindel_file.close()
    else:
        pindel_file = open(pindel_short_insertions_filename, "r")
        for line in pindel_file:
            fields = line.split()
            if len(fields) < 2 or fields[1] != "I":
                continue
            if int(fields[2]) < short_threshold:
                continue
            if float(fields[24]) >= v:
                chrom = fields[7]
                ostart = fields[12]
                oend = fields[13]
                length = fields[4]
                bed_line = "\t".join([chrom, ostart, oend, length])
                    #print bed_line.strip()
                calls_gte_threshold.append(bed_line)
        pindel_file.close()
        pindel_file = open(pindel_long_insertions_filename, "r")
        for line in pindel_file:
            fields = line.split()
            if len(fields) < 2 or fields[1] != "LI":
                continue
            if float(fields[6]) * float(fields[9]) >= v:
                chrom = fields[3]
                ostart = fields[4]
                oend = fields[7]
                length = 100
                bed_line = "\t".join([chrom, ostart, oend, length])
                #print bed_line.strip()
                calls_gte_threshold.append(bed_line)
        pindel_file.close()

    if sv_type == "DEL":
        (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_deletions(truth_filename, calls_gte_threshold, print_hits)
    else:
        (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_insertions(truth_filename, calls_gte_threshold, print_hits)
    if (qualified_calls > 0):
        tpr = float(matches) / (qualified_calls)
    else:
        tpr = "NA"
    if not print_hits:
        print "\t".join(map(str, [v, qualified_calls, matches, 0, short_calls, tpr]))
    
    
