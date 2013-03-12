package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.CloudbreakMapReduceBase;
import edu.ohsu.sonmezsysbio.cloudbreak.file.FaidxFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.util.MedianFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/11/13
 * Time: 4:29 PM
 */
public class GMMResultsToDeletionCallsReducer extends CloudbreakMapReduceBase implements Reducer<IntWritable, Text, Text, Text> {

    private static Logger log = Logger.getLogger(GMMResultsToDeletionCallsReducer.class);

    { log.setLevel(Level.DEBUG); }

    private String faidxFileName;
    FaidxFileHelper faix;

    int targetIsize;
    int targetIsizeSD;
    double lrThreshold;
    int medianFilterWindow;

    @Override
    public void reduce(IntWritable chromosome,
                       Iterator<Text> gmmScorerResultsIterator,
                       OutputCollector<Text, Text> textTextOutputCollector,
                       Reporter reporter) throws IOException {
        exportRegionsOverThresholdFromWig(gmmScorerResultsIterator, textTextOutputCollector, lrThreshold,
                medianFilterWindow, targetIsize, targetIsizeSD, Cloudbreak.VARIANT_TYPE_DELETION, (short) chromosome.get());

    }

    private void exportRegionsOverThresholdFromWig(Iterator<Text> gmmScorerResultsIterator,
                                                   OutputCollector<Text, Text> textTextOutputCollector,
                                                   double lrThreshold, int medianFilterWindow,
                                                   int targetIsize, int targetIsizeSD, String variantType,
                                                   short chromosome) throws IOException {
        Long lengthForChromName = faix.getLengthForChromName(faix.getNameForChromKey(chromosome));
        int numTiles = (int) Math.ceil(((double) lengthForChromName) / resolution);
        double[] values = new double[numTiles];;
        log.debug("computing results for chr " + chromosome + ", num tiles = " + numTiles);
        Map<String, double[]> extraWigFileValues = new HashMap<String, double[]>();

        int peakNum = 1;

        double[] muFileValues = new double[numTiles];;
        double[] w0Values = new double[numTiles];

        while (gmmScorerResultsIterator.hasNext()) {
            String[] fields = gmmScorerResultsIterator.next().toString().split("\t");
            long pos = Long.valueOf(fields[0]);
            if (pos > lengthForChromName) continue;
            double val = Double.valueOf(fields[1]);
            int tileNum = (int) pos / resolution;
            values[tileNum] = val;

            double muVal = Double.valueOf(fields[2]);
            muFileValues[tileNum] = muVal;

            double w0Val = Double.valueOf(fields[3]);
            w0Values[tileNum] = w0Val;
        }

        log.debug("applying median filter with window size " + medianFilterWindow);
        double[] filteredVals = MedianFilter.medianFilterValues(values, medianFilterWindow, lrThreshold);
        writePositiveRegions(filteredVals, textTextOutputCollector, faix.getNameForChromKey(chromosome), faix,
                resolution, peakNum, muFileValues, w0Values,
                targetIsize, targetIsizeSD, variantType);
    }

    private static int writePositiveRegions(double[] filteredVals, OutputCollector<Text, Text> textTextOutputCollector,
                                            String currentChromosome, FaidxFileHelper faidx, int resolution,
                                            int peakNum, double[] muFileValues, double[] w0FileValues,
                                            int targetIsize, int targetIsizeSD, String desiredVariantType) throws IOException {
        log.debug("writing positive regions");
        boolean usingMuValues = true;

        boolean inPositivePeak = false;
        long peakStart = 0;
        int idx = 0;
        double peakMax = 0;

        double muValSum = 0;
        double muValMin = 0;
        double muValMax = 0;

        double w0ValSum = 0;
        double w0ValMin = 0;
        double w0ValMax = 0;

        while (idx < filteredVals.length) {
            long pos = idx * resolution;

            // If we are filtering regions based on the estimated mean of the second component,
            // if the mean changes more by more than twice the SD of the library we break up the
            // prediction for deletions
            if (filteredVals[idx] > 0) {
                if (!Cloudbreak.VARIANT_TYPE_DELETION.equals(desiredVariantType) || (idx < 2 || Math.abs(muFileValues[idx] - muFileValues[(idx - 2)]) < 2 * targetIsizeSD)) {
                    if (!inPositivePeak) {
                        log.debug("beginning peak at " + pos);
                        peakStart = pos;
                        inPositivePeak = true;
                        muValSum = 0;
                        muValMin = Double.POSITIVE_INFINITY;
                        muValMax = Double.NEGATIVE_INFINITY;
                        w0ValSum = 0;
                        w0ValMin = Double.POSITIVE_INFINITY;
                        w0ValMax = Double.NEGATIVE_INFINITY;
                    }
                    peakMax = Math.max(peakMax, filteredVals[idx]);
                    muValSum += muFileValues[idx];
                    muValMin = Math.min(muValMin, muFileValues[idx]);
                    muValMax = Math.max(muValMax, muFileValues[idx]);
                    w0ValSum += w0FileValues[idx];
                    w0ValMin = Math.min(w0ValMin, w0FileValues[idx]);
                    w0ValMax = Math.max(w0ValMax, w0FileValues[idx]);

                } else {
                    if (inPositivePeak) {
                        log.debug("ending peak because of mu shift");
                        long endPosition = pos - 1;
                        long length = endPosition - peakStart;
                        double avgMu = muValSum * resolution / ((endPosition + 1) - peakStart);
                        peakNum = determineVariantTypeAndWriteLine(textTextOutputCollector, currentChromosome, resolution, peakNum,
                                targetIsize, usingMuValues, peakStart, peakMax, muValMin, muValMax, w0ValSum, w0ValMin,
                                w0ValMax, endPosition, length, avgMu, desiredVariantType);
                        inPositivePeak = false;
                        peakMax = 0;
                    }
                }
            } else {
                if (inPositivePeak) {
                    log.debug("ending peak because out of positive values");
                    long endPosition = pos - 1;
                    long length = endPosition - peakStart;
                    double avgMu = muValSum * resolution / ((endPosition + 1) - peakStart);
                    peakNum = determineVariantTypeAndWriteLine(textTextOutputCollector, currentChromosome, resolution, peakNum,
                            targetIsize, usingMuValues, peakStart, peakMax, muValMin, muValMax, w0ValSum, w0ValMin,
                            w0ValMax, endPosition, length, avgMu, desiredVariantType);
                    inPositivePeak = false;
                    peakMax = 0;
                }
            }
            idx = idx + 1;
        }
        if (inPositivePeak) {
            log.debug("ending peak because there are no more values to look at");
            long endPosition = faidx.getLengthForChromName(currentChromosome) - 1;
            if (endPosition < peakStart) return peakNum;
            long length = endPosition - peakStart;
            double avgMu = muValSum * resolution / ((endPosition + 1) - peakStart);
            peakNum = determineVariantTypeAndWriteLine(textTextOutputCollector, currentChromosome, resolution, peakNum,
                    targetIsize, usingMuValues, peakStart, peakMax, muValMin, muValMax, w0ValSum, w0ValMin,
                    w0ValMax, endPosition, length, avgMu, desiredVariantType);
        }
        return peakNum;
    }

    private static int determineVariantTypeAndWriteLine(OutputCollector<Text,Text> outputCollector, String currentChromosome, int resolution, int peakNum,
                                                        int targetIsize, boolean usingMuValues, long peakStart,
                                                        double peakMax, double muValMin, double muValMax, double w0ValSum,
                                                        double w0ValMin, double w0ValMax,
                                                        long endPosition, long length, double avgMu, String desiredVariantType) throws IOException {
        log.debug("validating peak num " + peakNum + ", start = " + peakStart + ", end = " + endPosition);
        String variantType = null;
        if (! usingMuValues) {
            variantType = Cloudbreak.VARIANT_TYPE_UNKNOWN;
        } else if (validDeletionPrediction(targetIsize, length, avgMu)) {
            variantType = Cloudbreak.VARIANT_TYPE_DELETION;
        } else if (validInsertionPrediction(targetIsize, avgMu)) {
            variantType = Cloudbreak.VARIANT_TYPE_INSERTION;
        }
        if (desiredVariantType == null || desiredVariantType.equals(variantType)) {
            log.debug("got desired variant type, writing");
            writeLine(outputCollector, currentChromosome, resolution, peakNum, peakStart, peakMax,
                    muValMin, muValMax,
                    w0ValSum, w0ValMin, w0ValMax, endPosition, avgMu, variantType);
            peakNum += 1;
        }
        return peakNum;
    }

    /**
     * deletions are only valid if:
     * the estimated mean of the second component is larger than the target insert size and
     * the length of the region is not different from the estimated mean by more than the target insert size
     */
    private static boolean validDeletionPrediction(int targetIsize, long predictedRegionLength, double avgMu) {
        log.debug("trying to validate avgMu: " + avgMu + " targetIsize " + targetIsize + " predicted length " + predictedRegionLength);
        return (avgMu > targetIsize && Math.abs(predictedRegionLength - avgMu) <= targetIsize);
    }

    /**
     * insertions are only valid if:
     * the estimated mean of the second component is smaller than the target insert size and
     */
    private static boolean validInsertionPrediction(int targetIsize, double avgMu) {
        return (avgMu < targetIsize);
    }

    private static void writeLine(OutputCollector<Text,Text> outputCollector, String currentChromosome, int resolution, int peakNum, long peakStart,
                                  double peakMax, double muValMin, double muValMax,
                                  double w0ValSum, double w0ValMin, double w0ValMax, long endPosition, double avgMu,
                                  String variantType)
            throws IOException {
        Text key = new Text(currentChromosome);
        StringBuilder val = new StringBuilder();
        val.append(peakStart).append("\t").append(endPosition).append("\t").append(peakNum).append("\t").append(peakMax);
        if (variantType != null) {
            val.append("\t").append(variantType);
        }
        val.append("\t").append(avgMu);
        val.append("\t").append(muValMin);
        val.append("\t").append(muValMax);
        val.append("\t").append(w0ValSum * resolution / ((endPosition + 1) - peakStart));
        val.append("\t").append(w0ValMin);
        val.append("\t").append(w0ValMax);

        outputCollector.collect(key, new Text(val.toString()));
    }


    @Override
    public void configure(JobConf job) {
        super.configure(job);
        faidxFileName = job.get("alignment.faidx");
        faix = new FaidxFileHelper(faidxFileName);
        targetIsize = Integer.parseInt(job.get("target.isize"));
        targetIsizeSD = Integer.parseInt(job.get("target.isizesd"));
        lrThreshold = Double.parseDouble(job.get("variant.lr.threshold"));
        medianFilterWindow = Integer.parseInt(job.get("variant.mfw"));
    }
}
