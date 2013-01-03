import sys

print "track name=\"PILEDUP_DELETION_SCORES\""

chrom = ""
for line in sys.stdin.readlines():
    fields = line.split()
    if (fields[0] != chrom):
        print "variableStep chrom=" + fields[0] + " span=100"
        print fields[1] + "\t" + fields[2]
        chrom = fields[0]
    else:
        print fields[1] + "\t" + fields[2]
