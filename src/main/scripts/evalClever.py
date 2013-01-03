#!/usr/bin/env python

import sys
import subprocess
import evalBedFile

# expects input from running clever with the -v flag

clever_filename = sys.argv[1]
truth_filename = sys.argv[2]

score_values = []

clever_file = open(clever_filename, "r")
for line in clever_file:
    fields = line.split(" ")

    score = float(fields[13])
    score_values.append(score)

clever_file.close()

unique_score_values = list(set(score_values))
unique_score_values.sort(reverse=True)

print "\t".join(["Thresh", "Calls", "TP", "WrongType", "Short", "TPR"])
for v in unique_score_values:
    calls_gte_threshold = []
    clever_file = open(clever_filename, "r")
    non_del_calls = 0
    for line in clever_file:
        fields = line.split(" ")
        if float(fields[13]) <= v:
            chrom = fields[0]
            ostart = fields[1]
            oend = fields[2]
            call_type = fields[3]
            if call_type != "DEL":
                non_del_calls += 1
                continue

            sv_len = int(oend) - int(ostart)
            bed_line = "\t".join([chrom, ostart, oend])
            #print bed_line.strip()
            calls_gte_threshold.append(bed_line)
    (qualified_calls, matches, short_calls) = evalBedFile.eval_bed(truth_filename, calls_gte_threshold)
    if (qualified_calls > 0):
        tpr = float(matches) / (qualified_calls)
    else:
        tpr = "NA"
    print "\t".join(map(str, [v, qualified_calls, matches, non_del_calls, short_calls, tpr]))
    
    
