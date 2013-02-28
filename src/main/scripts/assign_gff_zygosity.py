#!/usr/bin/env python

import sys
from random import *

input_filename = sys.argv[1]
homozygous_filename = sys.argv[2]
heterozygous_filename = sys.argv[3]

input_file = open(input_filename, "r")

hom_file = open(homozygous_filename, "w")
het_file = open(heterozygous_filename, "w")

for line in input_file:
    r = randint(0,1)
    if r == 0:
        hom_file.write(line)
    else:
        het_file.write(line)
        hom_file.write(line)


    
