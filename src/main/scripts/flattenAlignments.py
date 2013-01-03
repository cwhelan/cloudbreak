#!/usr/bin/env python

import sys
import subprocess

for line in sys.stdin:
    line = line[line.find("\t")+1:].strip()
    reads = line.split("\tSVP_READ\t")
    read1s = reads[0].split("\tSVP_ALIGNMENT\t")
    read2s = reads[1].split("\tSVP_ALIGNMENT\t")
    for a1 in read1s:
        for a2 in read2s:
            # todo: need to remove reads in the wrong orientation and/or too far apart
            print a1
            print a2
