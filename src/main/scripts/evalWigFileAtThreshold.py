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
cloudbreak_home = sys.argv[7]

#sys.stderr.write("quantile " + str(q) + "\n")
#temp_file_name = "tmp/tmp_" + str(q) + ".bed"
temp_file = tempfile.NamedTemporaryFile()
temp_file_name = temp_file.name

extract_regions_cmd = ['hadoop', 'jar', cloudbreak_home + 'cloudbreak-1.0-SNAPSHOT-exe.jar', 'extractPositiveRegionsFromWig', '--inputWigFile', wig_filename, '--outputBedFile', temp_file_name, '--name', "tmp_" + str(q), "--faidx", faidx_filename, "--threshold", str(q), "--medianFilterWindow", median_filter_window, "--muFile", mu_file]
subprocess.call(extract_regions_cmd)

num_predictions = 0

bed_lines = []
for line in open_file(temp_file_name):
    if line.startswith("track"):
        continue
    fields = line.split()
    length = int(fields[2]) - int(fields[1])
    avg_mu = float(fields[5])
    # todo fix this hardcoded tolerance
    if (avg_mu < 300 or abs(avg_mu - length) > 300):
        continue
    num_predictions += 1
    bed_line = line.strip()
    bed_lines.append(bed_line)
        
(qualified_calls, matches, short_calls) = evalBedFile.eval_bed(truth_filename, bed_lines)
tpr = float(matches) / (qualified_calls)
print "\t".join(map(str, [q, qualified_calls, matches, 0, 0, short_calls, tpr]))
