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
input_hdfs_dir = sys.argv[12]
print_hits = False
if len(sys.argv) == 14 and sys.argv[13] == "--printHits":
    print_hits = True


temp_file = tempfile.NamedTemporaryFile()
temp_file_name = temp_file.name

if sv_type == "DEL":
    cb_subcommand = "extractDeletionCalls"
    extract_regions_cmd = ['hadoop', 'jar', cloudbreak_home + '/lib/cloudbreak-${project.version}-exe.jar', cb_subcommand, '--name', "tmp_" + str(q), "--faidx", faidx_filename, "--threshold", str(q), "--medianFilterWindow", median_filter_window, "--targetIsize", target_isize, "--targetIsizeSD", target_isize_sd, "--inputHDFSDir", input_hdfs_dir, "--outputHDFSDir", "/user/whelanch/tmp/" + temp_file_name]
    subprocess.call(extract_regions_cmd)
    subprocess.call("hadoop dfs -cat /user/whelanch/tmp/" + temp_file_name + "/part* | sort -k1,1 -k2,2n > " + temp_file_name, shell=True)
else:
    cb_subcommand = "extractInsertionCalls"
    extract_regions_cmd = ['hadoop', 'jar', cloudbreak_home + '/lib/cloudbreak-${project.version}-exe.jar', cb_subcommand, '--name', "tmp_" + str(q), "--faidx", faidx_filename, "--threshold", str(q), "--medianFilterWindow", median_filter_window, "--targetIsize", target
                       _isize, "--targetIsizeSD", target_isize_sd, "--inputHDFSDir", input_hdfs_dir, "--outputHDFSDir", "/user/whelanch/tmp/" + temp_file_name]
    subprocess.call(extract_regions_cmd)
    subprocess.call("hadoop dfs -cat /user/whelanch/tmp/" + temp_file_name + "/part* | sort -k1,1 -k2,2n > " + temp_file_name, shell=True)

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
    print "calling eval Bed with lines: " + bed_line
    (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_deletions(truth_filename, bed_lines, print_hits)
else:
    (qualified_calls, matches, short_calls) = evalBedFile.eval_bed_insertions(truth_filename, bed_lines, print_hits)

tpr = float(matches) / (qualified_calls)
if not print_hits:
    print "\t".join(map(str, [q, qualified_calls, matches, 0, 0, short_calls, tpr]))
