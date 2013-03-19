#! /usr/bin/python

# This quick and dirty script reads the part-xxx from the hadoop
# output of ./getInsObs.sh and collates the records for each position
# in the alignment.
# Zak Shafran, Feb 2013

import sys, argparse, operator

parser = argparse.ArgumentParser(description='Extract insert sizes')
parser.add_argument('logRatioThresh', help='discard alignments when log(p(A*)) - log(p(A)) below this threshold', type=float)
logRatioThresh = parser.parse_args().logRatioThresh

posIdxIns = {}
for l in sys.stdin:
    chrsm, posn, jnk, insz, logpcor = l.strip().split()
    if posIdxIns.has_key(posn):
        posIdxIns[posn].append((insz,float(logpcor)))
    else:
        posIdxIns[posn] = [(insz,float(logpcor))]
        
idx = posIdxIns.keys()
idx.sort(key=int)
       
nTotal = 0
nDiscarded = 0
for posn in idx:
    insTuples = posIdxIns[posn]
    nTotal += len(insTuples)
    maxlogCor = max(insTuples, key=operator.itemgetter(1))[1]
    print posn,
    for insdat in insTuples:
        if maxlogCor - insdat[1] > logRatioThresh:
            nDiscarded += 1
            continue
        else: 
            print insdat[0],
    print ''

sys.stderr.write('Discarded: %.2f (%d / %d) of alignments\n' %(nDiscarded/nTotal,nDiscarded,nTotal))

