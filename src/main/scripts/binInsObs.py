#! /usr/bin/python

# A quick and dirty script to organize the alignments according to
# their positions. The output of map-reduce step in ./getInsObs.sh
# unfortunately has the alignments unsorted and spread randomly across
# 100 files. We bin them into blocks and store each block in a
# separate file for subsequent block level sort. 
# Zak Shafran, Feb 2013

import gzip, sys, argparse, operator, numpy as np

parser = argparse.ArgumentParser(description='Group into contiguous blocks')
parser.add_argument('maxIndex', help='Maximum position index', type=int)
parser.add_argument('blockSize', help='Maximum block size for each file', type=int)
maxidx = parser.parse_args().maxIndex
block = parser.parse_args().blockSize

numFp = np.int(np.ceil(np.float(maxidx)/np.float(block)))

ofp = {}
for i in range(numFp):
    ofp[i] = gzip.open('insObs/' + '%d' %i + '.block.gz', 'w')

for i in range(100):
    ifp = gzip.open('insObs/' + '%d' %i + '.dat.gz')
    for l in ifp:
        posn, obs = l.strip().split(' ', 1)
        i = int(posn)/block
        ofp[i].write('%s %s\n' %(posn, obs))

for i in range(numFp):
    ofp[i].close()
