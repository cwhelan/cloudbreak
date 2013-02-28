#!/usr/bin/env python

__author__ = 'cwhelan'

import sys
import subprocess
import evalBedFile
from math import *

# Arguments:
#
# modil file: the chrX.INDEL.txt
# Truth file: File with the true deletions

modil_filename = sys.argv[1]
truth_filename = sys.argv[2]
sv_type = sys.argv[3]


score_values = []
very_small_value = 2.09215640182e-318
too_short_call_length = 30

print_hits = False
if len(sys.argv) == 6 and sys.argv[4] == "--printHits":
    threshold = float(sys.argv[5])
    score_values.append(threshold)
    print_hits = True
else:
    modil_file = open(modil_filename, "r")
    for line in modil_file:
        if line.startswith("#"):
            continue
        fields = line.split("\t")
        if abs(int(float(fields[6]))) < too_short_call_length:
            continue
        score = float(fields[9])
        score_values.append(round(log(max(score, very_small_value))))
    modil_file.close()

unique_score_values = list(set(score_values))
unique_score_values.sort()

if not print_hits:
    print "\t".join(["Thresh", "Calls", "TP", "WrongType", "Short", "TPR"])
for v in unique_score_values:
    calls_gte_threshold = []
    modil_file = open(modil_filename, "r")
    other_type_calls = 0
    for line in modil_file:
        fields = line.split("\t")
        if abs(int(float(fields[6]))) < too_short_call_length:
            continue
        if round(log(max(float(fields[9]), very_small_value))) >= v:
        #            print "gte v"
            if (sv_type == "DEL" and not fields[5] == "1") or (sv_type == "INS" and not fields[4] == "1") :
                other_type_calls += 1
                continue
            calls_gte_threshold.append(line)

    bed_lines = []
    for line in calls_gte_threshold:
        fields = line.split("\t")
        # need to strip off the "chr"
        chrom = fields[1][3:len(fields[1])]
        if (sv_type == "DEL"):
            bed_line = "\t".join([chrom, fields[2], fields[3]])
        else:
            bed_line = "\t".join([chrom, fields[2], fields[3], str(abs(int(float(fields[6]))))])
        bed_lines.append(bed_line)

    if (sv_type == "DEL"):
        (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_deletions(truth_filename, bed_lines, print_hits)
    else:
        (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_insertions(truth_filename, bed_lines, print_hits)
    tpr = float(matches) / (qualified_calls)
    if not print_hits:
        print "\t".join(map(str, [v, qualified_calls, matches, other_type_calls, short_calls, tpr]))


