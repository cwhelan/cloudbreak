package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import com.google.common.primitives.Doubles;
import edu.ohsu.sonmezsysbio.cloudbreak.ReadGroupInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocationWithQuality;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;

import static org.apache.commons.math3.stat.StatUtils.mean;
import static org.apache.commons.math3.stat.StatUtils.sum;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 10/10/12
 * Time: 9:17 PM
 */
public class GenotypingGMMScorer {

    private static org.apache.log4j.Logger log = Logger.getLogger(GenotypingGMMScorer.class);

    { log.setLevel(Level.DEBUG);}

    public static final int MAX_COVERAGE = 200;

    private int minCoverage;
    private double maxLogMapqDiff;

    public void setMaxLogMapqDiff(double maxLogMapqDiff) {
        this.maxLogMapqDiff = maxLogMapqDiff;
    }

    public double getMaxLogMapqDiff() {
        return maxLogMapqDiff;
    }

    public int getMinCoverage() {
        return minCoverage;
    }

    public void setMinCoverage(int minCoverage) {
        this.minCoverage = minCoverage;
    }

    private double[] pointLikelihoods(double[] y, double mu, double sigma) {
        double[] pointLikelihoods = new double[y.length];
        for (int i = 0; i < y.length; i++) {
            pointLikelihoods[i] = lognormal(y[i], mu, sigma);
        }
        return pointLikelihoods;
    }

    private double lognormal(double y, double mu, double sigma) {
        return -1.0 * Math.log(sigma + Math.sqrt(2.0 * Math.PI)) + -1.0 / 2.0 * Math.pow(((y - mu) / sigma), 2);
    }

    double likelihood(double[] y, double[] w, double[] mu, double sigma) {
        double[][] pointLikelihoods = new double[w.length][y.length];
        for (int i = 0; i < w.length; i++) {
            pointLikelihoods[i] = pointLikelihoods(y, mu[i], sigma);
        }

        double sumWeightedLikelihoods = 0;
        for (int i = 0; i < y.length; i++) {
            double[] weightedPointLikelihoods = new double[w.length];
            for (int j = 0; j < w.length; j++) {
                weightedPointLikelihoods[j] = w[j] + pointLikelihoods[j][i];
            }
            sumWeightedLikelihoods += logsumexp(weightedPointLikelihoods);
        }
        return sumWeightedLikelihoods / y.length;
    }

    double logsumexp(double[] x) {
        double m = Doubles.max(x);
        double sumexp = 0;
        for (int i = 0; i < x.length; i++) {
            sumexp += Math.exp(x[i] - m);
        }
        double s = m + Math.log(sumexp);
        return s;
    }

    private double[][] gamma(double[] y, double[] w, double[] mu, double sigma) {
        double[][] gamma = new double[y.length][w.length];
        double[][] likelihoods = new double[y.length][w.length];
        for (int i = 0; i < y.length; i++) {
            for (int j = 0; j < w.length; j++) {
                likelihoods[i][j] = w[j] + lognormal(y[i], mu[j], sigma);
            }
            double total = logsumexp(likelihoods[i]);
            for (int j = 0; j < w.length; j++) {
                gamma[i][j] = likelihoods[i][j] - total;
            }
        }
        return gamma;
    }

    private double[] cacluateN(double[][] gamma) {
        double[] ns = new double[gamma[0].length];
        for (int j = 0; j < gamma[0].length; j++) {
            double[] temp = new double[gamma.length];
            for (int i = 0; i < gamma.length; i++) {
                temp[i] = gamma[i][j];
            }
            ns[j] = logsumexp(temp);
        }
        return ns;
    }

    private double[] updateW(double[] ns, double[] y) {
        double[] w = new double[ns.length];
        for (int j = 0; j < ns.length; j++) {
            w[j] = ns[j] - Math.log(y.length);
        }
        return w;
    }

    double updateMuForComponent(double[][] gamma, double[] y, double[] n, int component) {
        double numerator = 0;
        double[] lse = new double[y.length];
        for (int i = 0; i < y.length; i++) {
            lse[i] = gamma[i][component] + Math.log(y[i]);
        }
        numerator = logsumexp(lse);
        return Math.exp(numerator - n[component]);
    }

    private static class EMUpdates {
        double[] w;
        double[] mu;
        public double[] n;
        public double[][] gamma;

        @Override
        public String toString() {
            return "EMUpdates{" +
                    "w=" + w +
                    ", mu=" + mu +
                    ", n=" + n +
                    '}';
        }
    }

    private EMUpdates emStep(double[] y, double[] w, double[] mu, double sigma, int[] freeMus) {
        double[][] gamma = gamma(y, w, mu, sigma);
        double[] n = cacluateN(gamma);

        EMUpdates updates = new EMUpdates();
        updates.gamma = gamma;
        updates.mu = Arrays.copyOf(mu, mu.length);

        double[] wprime = updateW(n, y);
        updates.w = wprime;

        for (int k = 0; k < freeMus.length; k++) {
            int j = freeMus[k];
            double mujprime = updateMuForComponent(gamma, y, n, j);
            updates.mu[j] = mujprime;
        }
        updates.n = n;

        return updates;
    }

    public double[] nnclean(double[] y, List<Integer> ysWithCloseNeighbors) {
        double[] result = new double[ysWithCloseNeighbors.size()];
        for (int i = 0; i < ysWithCloseNeighbors.size(); i++) {
            result[i] = y[ysWithCloseNeighbors.get(i)];
        }
        return result;
    }

    public List<Integer> cleanYIndices(double[] y, double sigma, int m, int clusterCutoffInSDs) {
        List<Integer> ysWithCloseNeighbors = new ArrayList<Integer>();
        if (m < y.length) {
            // todo: there's a much better way to do this: sort ys and only calculate distances of neigbors
            double[][] dist = new double[y.length][y.length];
            for (int i = 0; i < y.length; i++) {
                for (int j = 0; j < y.length; j++) {
                    dist[i][j] = Math.abs(y[i] - y[j]);
                }
            }
            for (int i = 0; i < y.length; i++) {
                Arrays.sort(dist[i]);
                double distanceToMthNeighbor = dist[i][m];
                if (distanceToMthNeighbor < clusterCutoffInSDs * sigma) {
                    ysWithCloseNeighbors.add(i);
                }
            }
        }
        return ysWithCloseNeighbors;
    }

    private double dist(double x, double y) {
        return Math.abs(x - y);
    }

    public List<Integer> cleanYIndices2(double[] y, double sigma, int m, int clusterCutoffInSDs) {
        List<Integer> ysWithCloseNeighbors = new ArrayList<Integer>();
        if (m < y.length) {
            Arrays.sort(y);
            for (int i = 0; i < y.length; i++) {
                int offsetright = 0;
                int offsetleft = 0;
                double dist = 0;
                while (offsetleft + offsetright < m) {
                    if ((i + (offsetright + 1) >= y.length) ||
                            (i - (offsetleft + 1) >= 0 && dist(y[i], y[i - (offsetleft + 1)]) < dist(y[i], y[i + (offsetright + 1)]))) {
                        offsetleft += 1;
                        dist = dist(y[i], y[i - (offsetleft)]);
                    } else {
                        offsetright += 1;
                        dist = dist(y[i], y[i + (offsetright)]);
                    }
                }
                if (dist < clusterCutoffInSDs * sigma) {
                    ysWithCloseNeighbors.add(i);
                }
            }
        }
        return ysWithCloseNeighbors;
    }

    public GMMScorerResults estimate(double[] y, double[] initialW, double initialMu1, double sigma, double[] mappingScoreArray) {
        GMMScorerResults results = new GMMScorerResults();
        int maxIterations = 10;
        List<Integer> ysWithCloseNeighbors = cleanYIndices2(y, sigma, 2, 5);
        double[] yclean = nnclean(y, ysWithCloseNeighbors);

        if (yclean.length <= minCoverage) {
            results.w0 = -1;
            return results;
        }
        double[] cleanMappingScores = new double[ysWithCloseNeighbors.size()];
        for (int i = 0; i < ysWithCloseNeighbors.size(); i++) {
            cleanMappingScores[i] = mappingScoreArray[ysWithCloseNeighbors.get(i)];
        }
        double nodelOneComponentLikelihood = likelihood(yclean, new double[]{Math.log(1)}, new double[]{initialMu1}, sigma);

        double[] initialMu = new double[]{initialMu1,mean(yclean)};
        int i = 1;
        double[] w = initialW;
        double[] mu = initialMu;
        double l = likelihood(yclean, w, mu, sigma);
        EMUpdates updates;
        while(true) {
            updates = emStep(yclean, w, mu, sigma, new int[] {1});

            w = updates.w;
            mu = updates.mu;
            double lprime = likelihood(yclean, w, mu, sigma);
            i += 1;
            if (Math.abs(l - lprime) < 0.0001 || i > maxIterations) {
                break;
            }
            l = lprime;
        }
        results.lrHeterozygous = l - nodelOneComponentLikelihood;
        results.mu2 = mu[1];
        results.w0 = Math.exp(w[0]);

        return results;
    }

    private double weightByMappingScore(double[][] memberships, double[] mappingScoreArray, int component) {

        double[] weightedMemberships = new double[mappingScoreArray.length];
        for (int i = 0; i < mappingScoreArray.length; i++) {
            weightedMemberships[i] = memberships[i][component] + mappingScoreArray[i];
        }
        return logsumexp(weightedMemberships);
    }

    public GMMScorerResults reduceReadPairInfos(Iterator<ReadPairInfo> values, Map<Short, ReadGroupInfo> readGroupInfos, GenomicLocationWithQuality key) {
        List<Double> insertSizes = new ArrayList<Double>();
        List<Double> mappingScores = new ArrayList<Double>();
        double maxSD = 0;
        int targetIsize = 0;
        List<ReadGroupInfo> readgroups = new ArrayList<ReadGroupInfo>(readGroupInfos.values());
        if (readgroups.size() > 1) {
            targetIsize = readgroups.get(0).isize;
            for (int i = 1; i < readgroups.size(); i++) {
                if (readgroups.get(i).isize != targetIsize) {
                    throw new UnsupportedOperationException("GMM Reducer can't work with libraries with different insert sizes right now");
                }
            }
        }
        boolean first = true;
        double bestMappingQuality = 0;
        while (values.hasNext()) {
            ReadPairInfo rpi = values.next();
            if (log.isDebugEnabled() && key.chromosome == 0 && key.pos == 15000) {
                log.debug(rpi);
            }
            if (! first & (bestMappingQuality - rpi.pMappingCorrect > getMaxLogMapqDiff())) {
                break;
            }
            int insertSize = rpi.insertSize;
            short readGroupId = rpi.readGroupId;
            ReadGroupInfo readGroupInfo = readGroupInfos.get(readGroupId);
            insertSizes.add((double) (insertSize));
            mappingScores.add(rpi.pMappingCorrect);
            if (readGroupInfo.isizeSD > maxSD) {
                maxSD = readGroupInfo.isizeSD;
            }
            if (first) {
                targetIsize = readGroupInfo.isize;
                bestMappingQuality = rpi.pMappingCorrect;
                first = false;
            }
        }
        if (insertSizes.size() >= MAX_COVERAGE) {
            insertSizes = insertSizes.subList(0, MAX_COVERAGE);
        }
        double[] initialW = new double[]{Math.log(.5),Math.log(.5)};
        double[] insertSizeArray = new double[insertSizes.size()];
        for (int i = 0; i < insertSizes.size(); i++) {
            insertSizeArray[i] = insertSizes.get(i);
        }
        double[] mappingScoreArray = new double[mappingScores.size()];
        for (int i = 0; i < mappingScores.size(); i++) {
            mappingScoreArray[i] = mappingScores.get(i);
        }

        return estimate(insertSizeArray, initialW, targetIsize, maxSD, mappingScoreArray);
    }
}
