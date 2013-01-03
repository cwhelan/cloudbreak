package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import edu.ohsu.sonmezsysbio.cloudbreak.ReadGroupInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/23/12
 * Time: 9:52 AM
 */
public class WeightedBeliefDeltaScorer implements ReadPairInfoScorer {

    private static org.apache.log4j.Logger log = Logger
            .getLogger(WeightedBeliefDeltaScorer.class);

    //{ log.setLevel(Level.DEBUG); }

    public double reduceReadPairInfos(Iterator<ReadPairInfo> values, Map<Short, ReadGroupInfo> readGroupInfos) {
        LogNormalDistribution logNormalDistribution = new LogNormalDistribution(6, 0.6);

        double pDeletionPrior = Math.log(2432.0 / 2700000000.0);
        double pNoDeletionPrior = Math.log(1 - 2432.0 / 2700000000.0);

        double weightedSumOfPDeletions = Double.NEGATIVE_INFINITY;
        double weightedSumOfPNoDeletions = Double.NEGATIVE_INFINITY;
        log.debug("weightedSumOfPDeletions: " + weightedSumOfPDeletions);
        log.debug("weightedSumOfPNoDeletions: " + weightedSumOfPNoDeletions);

        boolean first = true;
        double bestQuality = 0;
        ArrayDeque<ReadPairInfo> bestRPIs = new ArrayDeque<ReadPairInfo>();
        while (values.hasNext()) {
            ReadPairInfo candidateReadPairInfo = values.next();
            log.debug("examining value: " + candidateReadPairInfo);
            if (first) {
                bestQuality=candidateReadPairInfo.pMappingCorrect;
                first=false;
            }
            if (bestQuality - candidateReadPairInfo.pMappingCorrect > 5) {
                log.debug("difference is bigger then 5, done adding values");
                break;
            }
            log.debug("adding " + candidateReadPairInfo);
            bestRPIs.addLast(new ReadPairInfo(candidateReadPairInfo.insertSize, candidateReadPairInfo.pMappingCorrect, candidateReadPairInfo.readGroupId));
            if (bestRPIs.size() > 1000) {
                break;
            }
        }

        Iterator<ReadPairInfo> goodPairIterator = bestRPIs.descendingIterator();
        while (goodPairIterator.hasNext()) {

            ReadPairInfo readPairInfo = goodPairIterator.next();
            int insertSize = readPairInfo.insertSize;
            double pMappingCorrect = readPairInfo.pMappingCorrect;
            log.debug("processing " + readPairInfo);
            short readGroupId = readPairInfo.readGroupId;

            ReadGroupInfo readGroupInfo = readGroupInfos.get(readGroupId);
            int targetIsize = readGroupInfo.isize;
            int targetIsizeSD = readGroupInfo.isizeSD;

            NormalDistribution normalDistribution = new NormalDistribution(targetIsize, targetIsizeSD);
            double pISgivenDeletion = Math.log(logNormalDistribution.density(insertSize));         // todo add fragment size
            double pISgivenNoDeletion = Math.log(normalDistribution.density(insertSize));
            log.debug("pISgivenDeletion: " + pISgivenDeletion);
            log.debug("pISgivenNoDeletion: " + pISgivenNoDeletion);

            if (insertSize > targetIsize + 10 * targetIsizeSD) {
                pISgivenNoDeletion = Math.log(normalDistribution.density(targetIsize + 10 * targetIsizeSD));
                log.debug("adj pISgivenNoDeletion: " + pISgivenNoDeletion);
            }
            double pDeletionGivenIS = pDeletionPrior + pISgivenDeletion;
            double pNoDeletionGivenIS = pNoDeletionPrior + pISgivenNoDeletion;

            double adjPDeletion = Math.exp(pMappingCorrect) + pDeletionGivenIS;
            double adjPNoDeletion = Math.exp(pMappingCorrect) + pNoDeletionGivenIS;

            log.debug("adjPDeletion: " + adjPDeletion);
            log.debug("adjPNoDeletion: " + adjPNoDeletion);
            weightedSumOfPDeletions =  logAdd(weightedSumOfPDeletions, adjPDeletion);
            weightedSumOfPNoDeletions = logAdd(weightedSumOfPNoDeletions, adjPNoDeletion);
            log.debug("weightedSumOfPDeletions: " + weightedSumOfPDeletions);
            log.debug("weightedSumOfPNoDeletions: " + weightedSumOfPNoDeletions);

        }

        double score = weightedSumOfPDeletions - weightedSumOfPNoDeletions;
        log.debug("score: " + score);
        return score;

    }

    // from https://facwiki.cs.byu.edu/nlp/index.php/Log_Domain_Computations
    public static double logAdd(double logX, double logY) {
        // 1. make X the max
        if (logY > logX) {
            double temp = logX;
            logX = logY;
            logY = temp;
        }
        // 2. now X is bigger
        if (logX == Double.NEGATIVE_INFINITY) {
            return logX;
        }
        // 3. how far "down" (think decibels) is logY from logX?
        //    if it's really small (20 orders of magnitude smaller), then ignore
        double negDiff = logY - logX;
        if (negDiff < -20) {
            return logX;
        }
        // 4. otherwise use some nice algebra to stay in the log domain
        //    (except for negDiff)
        return logX + java.lang.Math.log(1.0 + java.lang.Math.exp(negDiff));
    }


}
