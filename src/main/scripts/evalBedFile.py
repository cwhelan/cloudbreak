#!/usr/bin/env python

import sys
import subprocess

def eval_bed(truth_filename, calls, printhits=False):
    size_threshold = 40
    max_short_hit_length = 75
    bedtools_process = subprocess.Popen(["intersectBed", "-a", "stdin", "-b", truth_filename, "-loj"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    pstdout = bedtools_process.communicate("\n".join(calls) + "\n")[0]
    matches = 0
    short_hits = 0
    found_features = set()
    calls = 0
    valid_short_hit_calls = 0
    current_call = ""
    current_call_length = -1
    hit_for_current_call = False
    short_hit_for_current_call = False
    for line in pstdout.split("\n"):
        if line == "":
            continue
        fields = line.split("\t")
        call = "\t".join(fields[0:3])
        if (current_call == ""):
            current_call = call
            current_call_length = int(fields[2]) - int(fields[1])
        if (call != current_call):
            # process the call we just finsished reading lines for
            if hit_for_current_call:
                matches = matches + 1
                calls += 1
            elif short_hit_for_current_call:
                if (current_call_length <= max_short_hit_length):
                    short_hits += 1
                else:
                    calls += 1
            else:
                calls += 1

            # reset
            hit_for_current_call = False
            short_hit_for_current_call = False
            current_call = call
            current_call_length = int(fields[2]) - int(fields[1])

        if (fields[len(fields) - 3] != "."):
            found_feature = "\t".join(fields[(len(fields) - 3):len(fields)])
            if not found_feature in found_features:
                found_feature_length = int(fields[(len(fields) - 1)]) - int(fields[(len(fields) - 2)])
                if abs(current_call_length - found_feature_length) < 300:
                    found_features.add(found_feature)
                    if found_feature_length <= size_threshold:
                        short_hit_for_current_call = True
                    else:
                        hit_for_current_call = True
                        if printhits:
                            print current_call + "\t" + found_feature

    if hit_for_current_call:
        matches = matches + 1
        calls += 1
    elif short_hit_for_current_call:
        if (current_call_length <= max_short_hit_length):
            short_hits += 1
        else:
            calls += 1
    else:
        calls += 1

    return (calls, matches, short_hits)

# this script evaluates a series of bed lines from stdin against a truth file (passed as an argument) and returns the number of correct calls
if __name__ == "__main__":
    import sys

    printHits = False
    truth_file = sys.argv[1]
    calls_file = sys.argv[2]
    if (len(sys.argv) > 3) and sys.argv[3] == "--printHits":
        printHits = True

    # slurp all the bed lines into memory

    calls = []
    for line in open(calls_file, 'r'):
        calls.append(line)

    if printHits:
        eval_bed(sys.argv[1], calls, printHits)
    else:
        print eval_bed(sys.argv[1], calls, printHits)
