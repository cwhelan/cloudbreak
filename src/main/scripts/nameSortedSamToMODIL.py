__author__ = 'cwhelan'

# converts sam format to modil format
# only works on data from one chromsome for now, so please split first!

import sys

current_read_id = "NA"
read1_alignments = {}
read2_alignments = {}

def print_reads(read1_alignments, read2_alignments):
    for p1 in read1_alignments:
        for p2 in read2_alignments:
            if abs(p1-p2) <= 25000:
                a1 = read1_alignments[p1]
                a2 = read2_alignments[p2]
                if p1 < p2:
                    left_read = a1
                    right_read = a2
                else:
                    left_read = a2
                    right_read = a1
                left_fields = left_read.split("\t")
                right_fields = right_read.split("\t")
                left_flag = int(left_fields[1])
                right_flag = int(right_fields[1])
                if not left_flag & 0x10 and right_flag & 0x10:
                    left_read_start = int(left_fields[3])
                    left_read_end = left_read_start + len(left_fields[9])
                    right_read_start = int(right_fields[3])
                    right_read_end = right_read_start + len(right_fields[9])
                    span_of_mapping = right_read_end - left_read_start
                    print "\t".join(map(str, [left_read_end, right_read_start, left_fields[0], lib_insert_size - span_of_mapping, lib_insert_size, right_fields[0]]))


lib_insert_size = int(sys.argv[1])
for line in sys.stdin:
    fields = line.split("\t")
    read_pair_name = fields[0]
    if read_pair_name != current_read_id:
        if current_read_id != "NA":
            print_reads(read1_alignments, read2_alignments)
            read1_alignments = {}
            read2_alignments = {}
        current_read_id = read_pair_name
    flag = int(fields[1])
    pos = int(fields[3])
    if flag & 0x40:
        read1_alignments[pos] = line
    else:
        read2_alignments[pos] = line
print_reads(read1_alignments, read2_alignments)
