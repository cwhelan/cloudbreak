#!/usr/bin/env python

import sys
from cStringIO import StringIO
import random
import subprocess
import tempfile
import os
import sys
import evalBedFile

def open_file(wig_filename):
    if (wig_filename.endswith("gz")):
        p = subprocess.Popen(["gzcat",wig_filename],
                             stdout = subprocess.PIPE)
        wig_file = p.stdout
    else:
        wig_file = open(wig_filename, "r")
    return wig_file

q = float(sys.argv[1])
wig_filename = sys.argv[2]
truth_filename = sys.argv[3]
faidx_filename = sys.argv[4]
median_filter_window = sys.argv[5]
mu_file = sys.argv[6]
w0_file = sys.argv[7]
cloudbreak_home = sys.argv[8]
target_isize = sys.argv[9]
target_isize_sd = sys.argv[10]
sv_type = sys.argv[11]

temp_file = tempfile.NamedTemporaryFile()
temp_file_name = temp_file.name

if sv_type == "DEL":
    cb_subcommand = "extractDeletionCalls"
else:
    cb_subcommand = "extractInsertionCalls"
extract_regions_cmd = ['hadoop', 'jar', cloudbreak_home + '/lib/cloudbreak-${project.version}-exe.jar', cb_subcommand, '--inputWigFile', wig_filename, '--outputBedFile', temp_file_name, '--name', "tmp_" + str(q), "--faidx", faidx_filename, "--threshold", str(q), "--medianFilterWindow", median_filter_window, "--muFile", mu_file, "--targetIsize", target_isize, "--targetIsizeSD", target_isize_sd, "--w0File", w0_file]
subprocess.call(extract_regions_cmd)

num_predictions = 0

bed_lines = []
for line in open_file(temp_file_name):
    if line.startswith("track"):
        continue
    fields = line.split()
    length = int(fields[2]) - int(fields[1])
    num_predictions += 1
    if sv_type == "DEL":
        bed_line = line.strip()
    else:
        ins_length = int(int(target_isize) - float(fields[6]))
        bed_line = "\t".join([fields[0], fields[1], fields[2], str(ins_length)])
    bed_lines.append(bed_line)

if sv_type == "DEL":
    (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_deletions(truth_filename, bed_lines)
else:
    (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_insertions(truth_filename, bed_lines)

tpr = float(matches) / (qualified_calls)
print "\t".join(map(str, [q, qualified_calls, matches, 0, 0, short_calls, tpr]))
