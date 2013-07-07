package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.CloudbreakMapReduceBase;
import edu.ohsu.sonmezsysbio.cloudbreak.file.FaidxFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocation;
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
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/11/13
 * Time: 4:29 PM
 */
public class GMMResultsToVariantCallsReducer extends CloudbreakMapReduceBase implements Reducer<GenomicLocation, Text, Text, Text> {

    private static Logger log = Logger.getLogger(GMMResultsToVariantCallsReducer.class);

    { log.setLevel(Level.DEBUG); }

    FaidxFileHelper faix;

    int targetIsize;
    int targetIsizeSD;
    double lrThreshold;
    int medianFilterWindow;
    private String variantType;
    private boolean insNoCovFilter;

    @Override
    public void reduce(GenomicLocation genomicLocation,
                       Iterator<Text> gmmScorerResultsIterator,
                       OutputCollector<Text, Text> textTextOutputCollector,
                       Reporter reporter) throws IOException {
        callVariants(gmmScorerResultsIterator, textTextOutputCollector, lrThreshold,
                medianFilterWindow, targetIsize, targetIsizeSD, variantType, (short) genomicLocation.chromosome);

    }

    private void callVariants(Iterator<Text> gmmScorerResultsIterator,
                              OutputCollector<Text, Text> textTextOutputCollector,
                              double lrThreshold, int medianFilterWindow,
                              int targetIsize, int targetIsizeSD, String variantType,
                              short chromosome) throws IOException {
        Long lengthForChromName = faix.getLengthForChromName(faix.getNameForChromKey(chromosome));
        int numTiles = (int) Math.ceil(((double) lengthForChromName) / resolution);
        log.debug("computing results for chr " + chromosome + ", num tiles = " + numTiles);
        int peakNum = 1;

        log.debug("applying median filter with window size " + medianFilterWindow);
        FilteredValueSet[] filteredValues = MedianFilter.medianFilterValues(new GMMResultsIterator(gmmScorerResultsIterator),
                medianFilterWindow, lrThreshold, lengthForChromName, resolution);
        log.debug("got back filtered regions: lrs have " + filteredValues[0].values.keySet().size() + " keys");
        writePositiveRegions(filteredValues[0], textTextOutputCollector, faix.getNameForChromKey(chromosome),
                resolution, peakNum, filteredValues[1], filteredValues[2],
                targetIsize, targetIsizeSD, variantType, lrThreshold, faix.getLengthForChromName(faix.getNameForChromKey(chromosome)) - 1);
    }

    public static class GMMResult {
        public long pos;
        public double lrVal;
        public double w0val;
        public double muval;

        public GMMResult(long pos, double lrVal, double muval, double w0val) {
            this.pos = pos;
            this.lrVal = lrVal;
            this.w0val = w0val;
            this.muval = muval;
        }
    }

    public static class GMMResultsIterator {
        Iterator<Text> textIterator;
        String nextLine;

        public GMMResultsIterator(Iterator<Text> textIterator) {
            this.textIterator = textIterator;
            if (textIterator.hasNext()) nextLine = textIterator.next().toString();
        }

        public boolean hasNext() {
            return nextLine != null;
        }

        public GMMResult getNextResult(long pos) {
            if (nextLine != null) {
                String[] fields = nextLine.split("\t");
                if (pos == Long.valueOf(fields[0])) {
                    if (textIterator.hasNext()) {
                        nextLine = textIterator.next().toString();
                    } else {
                        nextLine = null;
                    }
                    return new GMMResult(pos, Double.valueOf(fields[1]), Double.valueOf(fields[2]), Double.valueOf(fields[3]));
                } else {
                    return new GMMResult(pos, 0, 0, -1);
                }
            } else {
                return new GMMResult(pos, 0, 0, -1);
            }
        }
    }

    public int writePositiveRegions(FilteredValueSet filteredVals, OutputCollector<Text, Text> textTextOutputCollector,
                                     String currentChromosome, int resolution,
                                     int peakNum, FilteredValueSet muFileValues, FilteredValueSet w0FileValues,
                                     int targetIsize, int targetIsizeSD, String desiredVariantType, double lrThreshold,
                                     long chromLength) throws IOException {
        log.debug("writing positive regions");
        boolean usingMuValues = true;

        boolean inPositivePeak = false;

        long peakStart = 0;
        int idx = 0;
        double peakMax = 0;

        double preceedingW0Val = -1;
        double succeedingW0Val = -1;

        double muValSum = 0;
        double muValMin = 0;
        double muValMax = 0;

        double w0ValSum = 0;
        double w0ValMin = 0;
        double w0ValMax = 0;

        double muFileValuesN = -1;
        double muFileValuesNminus1 = -1;
        double muFileValuesNminus2 = -1;

        while (idx < filteredVals.maxIndex) {
            long pos = idx * resolution;
            muFileValuesNminus2 = muFileValuesNminus1;
            muFileValuesNminus1 = muFileValuesN;


            muFileValuesN = muFileValues.getVal(idx);

            // If we are filtering regions based on the estimated mean of the second component,
            // if the mean changes more by more than twice the SD of the library we break up the
            // prediction for deletions
            if (filteredVals.getVal(idx) > 0) {
                if (!Cloudbreak.VARIANT_TYPE_DELETION.equals(desiredVariantType) ||
                        ! muHasChangedTooMuch(targetIsizeSD, idx, muFileValuesN, muFileValuesNminus2)) {
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
                        preceedingW0Val = idx == 0 ? -1 : w0FileValues.getVal(idx - 1);
                    }
                    peakMax = Math.max(peakMax, filteredVals.getVal(idx));
                    muValSum += muFileValuesN;
                    muValMin = Math.min(muValMin, muFileValuesN);
                    muValMax = Math.max(muValMax, muFileValuesN);
                    w0ValSum += w0FileValues.getVal(idx);
                    w0ValMin = Math.min(w0ValMin, w0FileValues.getVal(idx));
                    w0ValMax = Math.max(w0ValMax, w0FileValues.getVal(idx));

                } else {
                    if (inPositivePeak) {
                        log.debug("ending peak because of mu shift at idx " + idx + "; muFileValuesN = " + muFileValuesN + "; muFileValuesNminus2 = " + muFileValuesNminus2);
                        long endPosition = pos - 1;
                        long length = endPosition - peakStart;
                        double avgMu = muValSum * resolution / ((endPosition + 1) - peakStart);
                        succeedingW0Val = w0FileValues.getVal(idx);
                        peakNum = determineVariantTypeAndWriteLine(textTextOutputCollector, currentChromosome, resolution, peakNum,
                                targetIsize, usingMuValues, peakStart, peakMax, muValMin, muValMax, w0ValSum, w0ValMin,
                                w0ValMax, endPosition, length, avgMu, desiredVariantType, lrThreshold, preceedingW0Val, succeedingW0Val);
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
                    succeedingW0Val = w0FileValues.getVal(idx);
                    peakNum = determineVariantTypeAndWriteLine(textTextOutputCollector, currentChromosome, resolution, peakNum,
                            targetIsize, usingMuValues, peakStart, peakMax, muValMin, muValMax, w0ValSum, w0ValMin,
                            w0ValMax, endPosition, length, avgMu, desiredVariantType, lrThreshold, preceedingW0Val, succeedingW0Val);
                    inPositivePeak = false;
                    peakMax = 0;
                }
            }
            idx = idx + 1;
        }
        if (inPositivePeak) {
            log.debug("ending peak because there are no more values to look at");
            long endPosition = chromLength;
            if (endPosition < peakStart) return peakNum;
            long length = endPosition - peakStart;
            double avgMu = muValSum * resolution / ((endPosition + 1) - peakStart);
            succeedingW0Val = -1;
            peakNum = determineVariantTypeAndWriteLine(textTextOutputCollector, currentChromosome, resolution, peakNum,
                    targetIsize, usingMuValues, peakStart, peakMax, muValMin, muValMax, w0ValSum, w0ValMin,
                    w0ValMax, endPosition, length, avgMu, desiredVariantType, lrThreshold, preceedingW0Val, succeedingW0Val);
        }
        return peakNum;
    }

    public static boolean muHasChangedTooMuch(int targetIsizeSD, int idx, double muFileValueN, double muFileValueNminus2) {
        if (idx < 2) return false;
        // x is far from x - 3
        // x is far from x - 2
        // x - 1 is far from x - 2
        // x - 1 is far from x - 3
        // x and x - 1 are close
        // x - 2 and x - 3 are close
        return (idx >= 2 && Math.abs(muFileValueN - muFileValueNminus2) > 2 * targetIsizeSD);
    }

    private int determineVariantTypeAndWriteLine(OutputCollector<Text, Text> outputCollector, String currentChromosome, int resolution, int peakNum,
                                                        int targetIsize, boolean usingMuValues, long peakStart,
                                                        double peakMax, double muValMin, double muValMax, double w0ValSum,
                                                        double w0ValMin, double w0ValMax,
                                                        long endPosition, long length, double avgMu, String desiredVariantType, double lrThreshold,
                                                        double preceedingW0Val, double succeedingW0Val) throws IOException {
        log.debug("validating peak num " + peakNum + ", start = " + peakStart + ", end = " + endPosition);
        String variantType = null;
        if (! usingMuValues) {
            variantType = Cloudbreak.VARIANT_TYPE_UNKNOWN;
        } else if (validDeletionPrediction(targetIsize, length, avgMu, peakMax, lrThreshold)) {
            variantType = Cloudbreak.VARIANT_TYPE_DELETION;
        } else if (validInsertionPrediction(targetIsize, avgMu, preceedingW0Val, succeedingW0Val)) {
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
    private static boolean validDeletionPrediction(int targetIsize, long predictedRegionLength, double avgMu, double peakMax, double lrThreshold) {
        log.debug("trying to validate avgMu: " + avgMu + " targetIsize " + targetIsize + " predicted length " + predictedRegionLength);
        return (peakMax >= lrThreshold && avgMu > targetIsize && Math.abs(predictedRegionLength - avgMu) <= targetIsize);
    }

    /**
     * insertions are only valid if:
     * the estimated mean of the second component is smaller than the target insert size and
     */
    private boolean validInsertionPrediction(int targetIsize, double avgMu, double preceedingW0Val, double succeedingW0Val) {
        return (avgMu < targetIsize) && (! insNoCovFilter || peakNotFlankedByNoCovBin(preceedingW0Val, succeedingW0Val));
    }

    private static boolean peakNotFlankedByNoCovBin(double preceedingW0Val, double succeedingW0Val) {
        return (preceedingW0Val > -1) && (succeedingW0Val > -1);
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
        String faidxFileName = job.get("alignment.faidx");
        faix = new FaidxFileHelper(faidxFileName);
        targetIsize = Integer.parseInt(job.get("target.isize"));
        targetIsizeSD = Integer.parseInt(job.get("target.isizesd"));
        lrThreshold = Double.parseDouble(job.get("variant.lr.threshold"));
        medianFilterWindow = Integer.parseInt(job.get("variant.mfw"));
        variantType = job.get("variant.type");
        insNoCovFilter = Boolean.parseBoolean(job.get("variant.insNoCovFilter"));
    }
}
