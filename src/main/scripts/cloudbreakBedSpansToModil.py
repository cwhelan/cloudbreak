#!/usr/bin/env/python

# this script can convert the data produced by the SingleEndAlignmentsToBedSpansMapper to the input
# format requested by MoDIL:
#
# 1. Position of the end of the left read
# 2. Position of the start of the right read
#3. READ ID
# 4. mean of insert size - (span of mapping)
# (e.g. if insert size is 100 and matepair mapping spans 200, it's -100)
# 5. mean of insert size
# 6. MATEPAIR ID
#
# parameter: the expected mean internal insert size of the library

__author__ = 'cwhelan'

import sys

library_isize = sys.argv[1]

for line in sys.stdin:
    fields = line.split()
    lread_end = int(fields[6])
    rread_begin = int(fields[7])
    read_id = fields[3]
    isize_diff = library_isize - (rread_begin - lread_end)
    mp_id = read_id[0:read_id.index("/")]
    print("\t".join(map(str, [lread_end, rread_begin, read_id, isize_diff, library_isize, mp_id])))



