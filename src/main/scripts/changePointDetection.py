#! /usr/bin/python

# 
# This program detects change point using a segmental likelihood test
# where the number of model parameters in the numerator and
# denominator are the same. This has the advantage of being parameter
# free. If A and B are the segments on the left and right of the
# potential change point, then we estimate single Gaussians G_A(A) and
# G_B(B) as well as a 2-mixture Gaussian G_AB(AB). The likelihood
# ratio test is then simply LL(G_A(A)) LL(G_B(B)) / LL(G_AB(AB)). The
# ratio is gauranteed to be equal to or lower than 1.
#
# Currently, the Gaussians are hard-coded potentially, this can be
# more general where each G_A and G_B are also GMMs as long as the
# number of components in the denominator is equal to that of both the
# GMMs in the numerator. Similarly, the learning rate and other
# parameters for the GMM can be tuned for applications.
#
# The program expects the following arguments (example):
#
# ./changePointDetection.py 100 2499975 insObs/0.blck.srt.gz 100 7
#
# where 100 and 2499975 are starting and ending positions in the input
# file insObs/0.blck.srt.gz. The fourth argument specify the segmental
# window to the right and left of the potential break point (here 100
# base pairs). The fifth argument is a minimum occupancy cutoff for
# learning any Gaussians (here 7).
#
# Zak Shafran Feb, 2013
#

import gzip, sys, argparse, operator, numpy as np, pdb
import sklearn, operator
from sklearn import mixture 
# import matplotlib
# from matplotlib import pylab as plt

# gmm params
covtype = 'diag'
stopthresh = 0.001
covfloor = 0.001
n_iter = 100
n_restarts = 3
def learnGMM(ncomp):
    return sklearn.mixture.GMM(n_components=ncomp,
                               covariance_type = covtype,
                               random_state = None,
                               thresh = stopthresh,
                               min_covar = covfloor,
                               n_iter = n_iter,
                               n_init = n_restarts,
                               params = 'wmc',
                               init_params = 'wmc')

def getObs(seg, idx, c):
    obs = np.array([], dtype=np.uint)
    for p in idx:
        obs = np.append(obs,seg[p+c])
    return obs

parser = argparse.ArgumentParser(description='Detect change points in a given segment from a given file')
parser.add_argument('start', help='start of the segment', type=int)
parser.add_argument('end', help='end of the segment', type=int)
parser.add_argument('segfile', help='file containing the segment', type=str)
parser.add_argument('window', help='window length', type=int)
parser.add_argument('obscutoff', help='ignore segments fewer than threshold number of observations', type=int)

args = parser.parse_args()
startSeg = args.start
endSeg = args.end
segFile = args.segfile
window = args.window
ocutoff = args.obscutoff

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
sys.stderr.write("# Segment: beg = %d, end = %d, min = %d, max = %d\n"
                 %(startSeg, lastPosn, minIns, maxIns))

t = seg.keys() 
t.sort(key=int)
obsPosn = np.zeros((lastPosn-startSeg+1), dtype=np.uint8)
for posn in t:
    obsPosn[posn-startSeg] = 1

buf = []
c = startSeg
tref = []
score = []
for k, idx in enumerate(t):
    i = int(idx)
    if i-window < startSeg:
        continue
    if i+window > lastPosn:
        continue
    obsA = getObs(seg, np.where(obsPosn[i-window-c:i-c]==1)[0], c)
    obsB = getObs(seg, np.where(obsPosn[i-c:i+window-c]==1)[0], i)
    if len(obsA) < ocutoff or len(obsB) < ocutoff:
        continue
    gmmA = learnGMM(1)
    gmmA.fit(obsA)
    # The likelihood evaluation for lhA and lhB can be potentially
    # simplified (faster) by a closed form solution using the
    # covariance and the scatter matrix.
    lhA = np.sum(gmmA.score(obsA))
    gmmB = learnGMM(1)
    gmmB = gmmB.fit(obsB)
    lhB = np.sum(gmmB.score(obsB))
    gmmAB = learnGMM(2)
    obsAB = np.append(obsA, obsB)
    gmmAB = gmmAB.fit(obsAB)
    lhAB = np.sum(gmmB.score(obsAB))
    tref.append(idx)
    score.append((lhA + lhB) - lhAB)
    print "%d %.3f" %(idx, (lhA + lhB) - lhAB)

# plt.plot(tref, score)
# plt.show()
