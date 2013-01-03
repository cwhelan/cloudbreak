package edu.ohsu.sonmezsysbio.cloudbreak;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 2/28/12
 * Time: 2:54 PM
 */
public abstract class PairedAlignmentScorer {

    private static org.apache.log4j.Logger logger = Logger.getLogger(PairedAlignmentScorer.class);

    //{ logger.setLevel(Level.DEBUG); }

    public boolean validateInsertSize(int insertSize, String readPairId, Integer maxInsertSize1) {
        if (insertSize == 0) return false;
        if (insertSize > maxInsertSize1) {
            if (logger.isDebugEnabled()) {
                logger.debug("Pair " + readPairId + ": Insert size would be greater than " + maxInsertSize1 + " - skipping");
            }
            return false;
        }
        return true;
    }

    public abstract double computeDeletionScore(int insertSize, Double targetIsize, Double targetIsizeSD, Double pMappingCorrect);

    public boolean validateMappingOrientations(AlignmentRecord record1, AlignmentRecord record2, boolean matePairs) {
        if (matePairs) {
            if (record1.isForward() && ! record2.isForward()) {
            } else if (!record1.isForward() && record2.isForward()) {
            } else {
                return false;
            }
        } else {
            if (record1.isForward() && ! record2.isForward()) {
                if (record1.getPosition() > record2.getPosition()) {
                    logger.debug("r1 forward; r2 back; r1 pos > r2 pos");
                    return false;
                }
            } else if (!record1.isForward() && record2.isForward()) {
                if (record1.getPosition() < record2.getPosition()) {
                    logger.debug("r1 back; r2 forward; r1 pos < r2 pos");
                    return false;
                }
            } else {
                logger.debug("not ((r1 forward and not r2 forward) or (not r1 forward and r2 forward))");
                return false;
            }
        }
        return true;
    }

    public boolean isMatePairNotSmallFragment(AlignmentRecord record1, AlignmentRecord record2) {
        boolean matePair = false;
        if (record1.isForward() && ! record2.isForward()) {
            if (record1.getPosition() - record2.getPosition() > 0) matePair = true;
            if (record1.getPosition() - record2.getPosition() < 0 &&
                    record1.getPosition() - record2.getPosition() > -500) matePair = false;
        } else if (!record1.isForward() && record2.isForward()) {
            if (record1.getPosition() - record2.getPosition() < 0) matePair = true;
            if (record1.getPosition() - record2.getPosition() > 0 &&
                    record1.getPosition() - record2.getPosition() < 500) matePair = false;
        }
        return matePair;
    }

}
