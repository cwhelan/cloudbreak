#!/usr/bin/env python

import sys
import subprocess
import evalBedFile

gasv_filename = sys.argv[1]
truth_filename = sys.argv[2]

score_values = []

gasv_file = open(gasv_filename, "r")
for line in gasv_file:
    if line.startswith("#"):
        continue
    fields = line.split("\t")
    call_type = fields[7]
    if call_type != "D":
        continue

    final_weighted_score = float(fields[5])
    score_values.append(final_weighted_score)
    # chrom = fields[1]
    # ostart = fields[2].split(",")[0]
    # oend = fields[4].split(",")[1]            
    # bed_line = "\t".join([chrom, ostart, oend])
    # print bed_line

gasv_file.close()

unique_score_values = list(set(score_values))
unique_score_values.sort()

print "\t".join(["Thresh", "Calls", "TP", "WrongType", "Short", "TPR"])
for v in unique_score_values:
    calls_gte_threshold = []
    gasv_file = open(gasv_filename, "r")
    non_del_calls = 0
    for line in gasv_file:
        if line.startswith("#"):
            continue
        fields = line.split("\t")
        call_type = fields[7]
        if float(fields[5]) >= v:
            if call_type != "D":
                non_del_calls += 1
                continue
            chrom = fields[1]
            ostart = fields[2].split(",")[0]
            oend = fields[4].split(",")[1]
            sv_len = int(oend) - int(ostart)
            bed_line = "\t".join([chrom, ostart, oend])
            #print bed_line.strip()
            calls_gte_threshold.append(bed_line)

    (qualified_calls, matches, short_calls) = evalBedFile.eval_bed(truth_filename, calls_gte_threshold)
    tpr = float(matches) / (qualified_calls)
    print "\t".join(map(str, [v, qualified_calls, matches, non_del_calls, short_calls, tpr]))

    
