#! /usr/bin/python

# This program is meant for visualizing the raw distribution of insert
# sizes at different positions (numerically sorted) in the
# chromosome. The x-axis in the plot corresponds to the position and
# the y-axis the insert size with the intensity showing the number of
# insert sizes. Additionally, optional datafiles can be plotted on the
# same x-axis to concomittantly view results for example from the
# ./changePointDetection.py detection script. One caveat, the script
# assumes the number of positions are the same in both files. This can
# be fixed by indexing the array 'segres' with position, now it is
# just a list.
# 
# Zak Shafran, Feb 2013
# 
# To do: fix the x-axis to display actual positions (see [**])

import gzip, sys, argparse, operator, numpy as np, pdb
import matplotlib
from matplotlib import pylab as plt

def setAxes(plt, posn):
    ax = plt.gca()
    ax.set_ylim(ax.get_ylim()[::-1])
    xticks = ax.get_xticks()
    if len(xticks) > 10:
        plt.locator_params(axis = 'x', nbins = 10)
    if len(ax.get_yticks()) > 5:
        plt.locator_params(axis = 'x', nbins = 5)
    # Need to create len(xtick) xtick labels and then set [**]
    # ax.set_xticklabels(['a1','b2','c3','d4','e5','f6','g7','h8','i9'])

# assuming the data is ordered and there are no missing indices
def grabSegment(startSeg, lastPosn, data):
    n = data.shape[0]
    m = lastPosn - startSeg
    out = []
    for i in range(n):
        if i > lastPosn:
            break
        if (data[i,0] >= startSeg) and (data[i,0] < lastPosn):
            out.append(data[i, 1])
    return out

parser = argparse.ArgumentParser(description='Plot the insert size in a segment from a given insert size file')
parser.add_argument('start', help='start of the segment', type=int)
parser.add_argument('end', help='end of the segment', type=int)
parser.add_argument('segfile', help='file containing the segment', type=str)
parser.add_argument('datafile', help='optional processed data file(s) for subplot(s)', nargs='*')

args = parser.parse_args()
startSeg = args.start
endSeg = args.end
segFile = args.segfile
dataFile = args.datafile
if dataFile:
    nplots = len(dataFile) + 1

fp = gzip.open(segFile)
seg = {}
minIns = -1
maxIns = -1
reachedEnd = -1
for l in fp:
    toks = l.strip().split()
    posn = int(toks.pop(0))
    if posn < startSeg:
        continue
    if posn > endSeg:
        reachedEnd = 1
        break
    insSizes = map(int, toks)
    sizes = []
    for insSize in insSizes:
        if insSize < minIns or minIns < 0:
            minIns = insSize
        if insSize > maxIns or maxIns < 0:
            maxIns = insSize            
        sizes.append(insSize)
    seg[posn] = sizes
    lastPosn = posn

if not reachedEnd:
    sys.stderr.write("Warning: EOF reached before end of segment!")
sys.stderr.write("Segment: beg = %d, end = %d, min = %d, max = %d\n"
                 %(startSeg, lastPosn, minIns, maxIns))

m = maxIns - minIns
data = np.zeros((m,len(seg)))
t = seg.keys() 
t.sort(key=int)
for i, posn in enumerate(t):
    insSizes = seg[posn]
    for ins in insSizes:
        data[ins - minIns - 1,i] += 1

ax1 = plt.subplot(211) #nplots,1,1)
plt.imshow(data, aspect='auto', cmap=matplotlib.cm.hot, 
           extent=[0, len(seg), m, 0])
setAxes(plt, t)

if not dataFile:
    cb = plt.colorbar(shrink=0.5)
    cb.set_ticks([0, np.max(data)], ['0', str(np.max(data))])
else:
    for p, f in enumerate(dataFile):
        result = np.loadtxt(f)
        segres = grabSegment(startSeg, lastPosn, result)
        plt.subplot(212, sharex=ax1) # nplots, 1, p+2, 
        plt.plot(segres)
        plt.xlim([0,len(t)])

plt.show()

