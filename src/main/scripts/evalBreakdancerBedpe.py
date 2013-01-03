#!/usr/bin/env python

import sys
import subprocess

breakdancer_filename = sys.argv[1]
truth_filename = sys.argv[2]

score_values = []

breakdancer_file = open(breakdancer_filename, "r")
for line in breakdancer_file:
    fields = line.split("\t")
    final_weighted_score = float(fields[7])
    score_values.append(final_weighted_score)
breakdancer_file.close()

unique_score_values = list(set(score_values))
unique_score_values.sort()

print "\t".join(["Thresh", "Calls", "TP", "TPR", "WrongType"])
for v in unique_score_values:
    calls_gte_threshold = []
    breakdancer_file = open(breakdancer_filename, "r")
    wrong_type = 0
    for line in breakdancer_file:
        fields = line.split("\t")
        if float(fields[7]) >= v:
            s1 = fields[8]
            s2 = fields[9]
            if not ((s1 == "+" and s2 == "-") or (s1 == "-" and s2 == "+")):
                wrong_type += 1
                continue
            sv_len = int(fields[5]) - int(fields[1])
            calls_gte_threshold.append(line)
    bedtoolsProcess = subprocess.Popen(["pairToBed", "-type", "ispan",  "-a", "stdin", "-b", truth_filename], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    bedpe_lines = ""
    for hline in calls_gte_threshold:
        bedpe_lines = bedpe_lines + hline + "\n"
#        bedtoolsProcess.stdin.write(hline)
#    bedtoolsProcess.stdin.close()
    pstdout = bedtoolsProcess.communicate(bedpe_lines)[0]
    matches = 0
    for line in pstdout.split("\n"):
        #print line
        matches += 1
    bedtoolsProcess.stdout.close()
    print "\t".join(map(str, [v, len(calls_gte_threshold), matches, float(matches) / (len(calls_gte_threshold)), wrong_type]))
    
    
