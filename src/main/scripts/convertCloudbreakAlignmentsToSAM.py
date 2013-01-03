#!/usr/bin/env python

import sys
import subprocess

for line in sys.stdin:
    line = line[line.find("\t")+1:]
    reads = line.split("SVP_READ")
    read1s = read[0].split("SVP_ALIGN")
    read2s = read[1].split("SVP_ALIGN")
    for a in read1s:
        paired_flag = 0x0001
        first_flag = 0x0040
        fields = a.split("\t")
        strand = fields[9] # should be 13 if not extended native format??
        
