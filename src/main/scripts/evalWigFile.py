#!/usr/bin/env python

import sys
import random
import subprocess
import tempfile
import os
from multiprocessing import Pool
import sys
import math
from cStringIO import StringIO
import time

wig_filename = sys.argv[1]
truth_filename = sys.argv[2]
faidx_filename = sys.argv[3]
medianFilterWindow = sys.argv[4]
lower_threshold = float(sys.argv[5])
mu_filename = sys.argv[6]

cbhome = sys.argv[7]

def open_file(wig_filename):
    if (wig_filename.endswith("gz")):
        sys.stderr.write("opening with subprocess\n")
        p = subprocess.Popen(["gzcat",wig_filename],
                             stdout = subprocess.PIPE)
        wig_file = p.stdout
    else:
        wig_file = open(wig_filename, "r")
    return wig_file

values_above_threshold = []
wig_file = open_file(wig_filename)
i = 0
for line in wig_file:
    if line.startswith("track") or line.startswith("variable"):
        continue
    val = round(float(line.split()[1]),5)
    # gah this was soooooooo sloooooooow
    #val = Decimal(line.split()[1]).quantize(Decimal('.00001'), rounding=ROUND_UP)
    if not math.isnan(val) and val > lower_threshold:
        values_above_threshold.append(val)
    i = i + 1
    if i % 1000000 == 0:
        sys.stderr.write("processed 1000000 wig lines + " + str(time.time()) + "\n")
wig_file.close()

sys.stderr.write("values above threshold: " + str(len(values_above_threshold)) + "\n")

values_above_threshold = list(set(values_above_threshold))
values_above_threshold.sort()

num_quantiles = 50
quantiles = [0] * (num_quantiles + 1)
q_num = 0

for i in xrange(len(values_above_threshold)):    
    if i % (len(values_above_threshold) / num_quantiles) == 0:
        if (q_num > num_quantiles):
            continue
#        sys.stderr.write("i: " + str(i))
        qv = values_above_threshold[i]
#        sys.stderr.write("q_num: " + str(q_num) + " = " + str(qv))
        quantiles[q_num] = qv
        #print "quantiles " + str(q_num) + " = " + str(values_above_threshold[i])
        q_num += 1
    
sys.stderr.write(str(quantiles))
sys.stderr.write("\n")

def process_quantile(q):
    eval_at_q_cmd = ['python', cbhome + 'src/main/scripts/evalWigFileAtThreshold.py', str(q), wig_filename, truth_filename, faidx_filename, medianFilterWindow, mu_filename, cbhome + 'target/']
    #print eval_at_q_cmd
    result = subprocess.Popen(eval_at_q_cmd, stdout=subprocess.PIPE).communicate()[0]
    result_fields = result.split()
    num_predictions = int(result_fields[1])
    num_matches = int(result_fields[2])
    num_short_calls = int(result_fields[5])
    tpr = float(result_fields[6])
    if num_predictions == 0:
        print "Warning: zero predictions in line: " + result
        return (q, 0, 0, 0, 0, 0)
    else:
        return (q, num_predictions, num_matches, 0, num_short_calls,tpr)
    
p=Pool(3)
results = p.map(process_quantile, quantiles)

print "\t".join(["Thresh", "Calls", "TP", "Wrong Type", "Short", "TPR"])
for q in results:
    print "\t".join(map(str, q))
