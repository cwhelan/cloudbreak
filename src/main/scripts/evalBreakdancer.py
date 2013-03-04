#!/usr/bin/env python

import sys
import subprocess
import evalBedFile

# Arguments:
#
# Breakdancer file: the bd_out.txt file generated by breakdancer
# Truth file: File with the true deletions

breakdancer_filename = sys.argv[1]
truth_filename = sys.argv[2]
sv_type = sys.argv[3]

score_values = []

print_hits = False
print_bed = False

if len(sys.argv) == 6 and sys.argv[4] == "--printHits":
    threshold = float(sys.argv[5])
    score_values.append(threshold)
    print_hits = True
elif len(sys.argv) == 6 and sys.argv[4] == "--printBed":
    threshold = float(sys.argv[5])
    score_values.append(threshold)
    print_bed = True

else:
    breakdancer_file = open(breakdancer_filename, "r")
    for line in breakdancer_file:
        if line.startswith("#"):
            continue
        fields = line.split("\t")
        score = float(fields[9])
        score_values.append(score)
    breakdancer_file.close()

unique_score_values = list(set(score_values))
unique_score_values.sort()

if not print_hits and not print_bed:
    print "\t".join(["Thresh", "Calls", "TP", "WrongType", "Short", "TPR"])
for v in unique_score_values:
    calls_gte_threshold = []
    breakdancer_file = open(breakdancer_filename, "r")
    other_type_calls = 0
    # sometimes Breakdancer makes nonsensical calls; deletions with ends on different chromosomes, we have to
    # track those separately here
    bad_calls = 0
    for line in breakdancer_file:
#        print line
        fields = line.split("\t")
        if line.startswith("#"):
#            print "comment!"
            continue
        if float(fields[9]) >= v:
#            print "gte v"
            if not (fields[6] == sv_type):
                other_type_calls += 1
                continue
            if (fields[0] != fields[3]):
                bad_calls += 1
                continue
            calls_gte_threshold.append(line)

    bed_lines = []
    for line in calls_gte_threshold:
        fields = line.split("\t")
        if (sv_type == "DEL"):
            bed_line = "\t".join([fields[0], fields[1], fields[4]])
        else:
            bed_line = "\t".join([fields[0], fields[1], fields[4], str(abs(int(fields[7])))])
        bed_lines.append(bed_line)

    if print_bed:
        print "\n".join(bed_lines)
        continue

    if (sv_type == "DEL"):
        (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_deletions(truth_filename, bed_lines, print_hits)
    else:
        (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_insertions(truth_filename, bed_lines, print_hits)
    qualified_calls += bad_calls
    tpr = float(matches) / (qualified_calls)
    if not print_hits:
        print "\t".join(map(str, [v, qualified_calls, matches, other_type_calls, short_calls, tpr]))


