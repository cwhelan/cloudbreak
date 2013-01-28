package edu.ohsu.sonmezsysbio.cloudbreak.file;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/19/12
 * Time: 3:28 PM
 */

/**
 * Contains multiple commands for reading from and writing to Wig files.
 */
public class WigFileHelper {

    private static org.apache.log4j.Logger logger = Logger.getLogger(WigFileHelper.class);

    public static void exportRegionsOverThresholdFromWig(String name, BufferedReader wigFileReader, BufferedWriter bedFileWriter,
                                                         Double threshold, FaidxFileHelper faidx, int medianFilterWindow
                                                         ) throws IOException {
        exportRegionsOverThresholdFromWig(name, wigFileReader, bedFileWriter, threshold, faidx, medianFilterWindow, null, null,
                new ArrayList<String>(), new HashMap<String, BufferedReader>(), -1, -1);

    }

    public static void exportRegionsOverThresholdFromWig(String outputPrefix, BufferedReader wigFileReader,
                                                         BufferedWriter bedFileWriter, double threshold,
                                                         FaidxFileHelper faidx, int medianFilterWindow,
                                                         List<String> extraFileNames, Map<String, BufferedReader> extraWigFileReaders) throws IOException {
        exportRegionsOverThresholdFromWig(outputPrefix, wigFileReader, bedFileWriter, threshold, faidx, medianFilterWindow, null, null,
                extraFileNames, extraWigFileReaders, -1, -1);
    }

    public static void exportRegionsOverThresholdFromWig(String outputPrefix, BufferedReader wigFileReader,
                                                         BufferedWriter bedFileWriter, double threshold,
                                                         FaidxFileHelper faidx, int medianFilterWindow,
                                                         String muFile, BufferedReader muFileReader,
                                                         List<String> extraFileNames, Map<String, BufferedReader> extraWigFileReaders,
                                                         int targetIsize, int targetIsizeSD)
            throws IOException {
        String trackName = outputPrefix + " peaks over " + threshold;
        bedFileWriter.write("track name = \"" + trackName + "\"\n");

        String line;
        String currentChromosome = "";

        double[] values = null;
        Map<String, double[]> extraWigFileValues = new HashMap<String, double[]>();

        int resolution = 0;
        int peakNum = 1;

        boolean readingMuFile = ! (muFile == null);
        String muFileLine = null;
        double[] muFileValues = null;

        Map<String, String> extraWigLines = new HashMap<String, String>();

        while ((line = wigFileReader.readLine()) != null) {

            if (readingMuFile) {
                muFileLine = muFileReader.readLine();
            }
            for (String extraWigFile : extraWigFileReaders.keySet()) {
                extraWigLines.put(extraWigFile, extraWigFileReaders.get(extraWigFile).readLine());
            }
            if (line.startsWith("track")) {
                continue;
            }
            if (line.startsWith("variableStep")) {

                if (values != null) {
                    double[] filteredVals = medianFilterValues(values, medianFilterWindow, threshold);
                    peakNum = writePositiveRegions(filteredVals, bedFileWriter, currentChromosome, faidx, resolution,
                            peakNum, muFileValues, extraFileNames, extraWigFileValues, targetIsize, targetIsizeSD);
                }
                currentChromosome = line.split(" ")[1].split("=")[1];
                resolution = Integer.valueOf(line.split(" ")[2].split("=")[1]);
                int numTiles = (int) Math.ceil(((double) faidx.getLengthForChromName(currentChromosome)) / resolution);
                values = new double[numTiles];
                if (readingMuFile) {
                    muFileValues = new double[numTiles];
                }
                for (String extraWigFile : extraWigFileReaders.keySet()) {
                    extraWigFileValues.put(extraWigFile, new double[numTiles]);
                }

            } else {
                String[] fields = line.split("\t");
                if (fields.length < 2) {
                    throw new RuntimeException("Failed to parse line: " + line);
                }
                long pos = Long.valueOf(fields[0]);
                if (pos > faidx.getLengthForChromName(currentChromosome)) continue;
                double val = Double.valueOf(fields[1]);
                int tileNum = (int) pos / resolution;
                values[tileNum] = val;

                if (readingMuFile) {
                    double muVal = parseExtraWigLine(line, muFile, muFileLine);
                    muFileValues[tileNum] = muVal;
                }

                for (String extraWigFile : extraWigFileReaders.keySet()) {
                    double extraVal = parseExtraWigLine(line, extraWigFile, extraWigLines.get(extraWigFile));
                    extraWigFileValues.get(extraWigFile)[tileNum] = extraVal;

                }
            }
        }
        double[] filteredVals = medianFilterValues(values, medianFilterWindow, threshold);
        writePositiveRegions(filteredVals, bedFileWriter, currentChromosome, faidx, resolution, peakNum, muFileValues, extraFileNames,
                extraWigFileValues, targetIsize, targetIsizeSD);

    }

    private static double parseExtraWigLine(String mainLine, String fileName, String extraWigLine) {
        String[] extraFields = extraWigLine.split("\t");
        if (extraFields.length < 2) {
            throw new RuntimeException("Failed to parse line in " + fileName + ": " + mainLine);
        }
        return Double.valueOf(extraFields[1]);
    }

    private static double[] medianFilterValues(double[] values, int medianFilterWindow, double threshold) {
        double[] filteredValues = new double[values.length];
        int idx = 0;
        while (idx <= medianFilterWindow / 2) {
            filteredValues[idx] = values[idx] > threshold ? values[idx] : 0.0;
            idx++;
        }

        while (idx < filteredValues.length - medianFilterWindow / 2) {
            double[] filterWindow = Arrays.copyOfRange(values, idx - medianFilterWindow / 2,
                    idx + medianFilterWindow / 2 + 1);
            Arrays.sort(filterWindow);
            double medianValue = filterWindow[medianFilterWindow / 2];
            if (medianValue > threshold) {
                filteredValues[idx] = values[idx];
            } else {
                filteredValues[idx] = 0;
            }
            idx++;
        }

        while (idx < filteredValues.length) {
            filteredValues[idx] = values[idx] > threshold ? values[idx] : 0.0;
            idx++;
        }
        return filteredValues;
    }

    private static int writePositiveRegions(double[] filteredVals, BufferedWriter bedFileWriter,
                                            String currentChromosome, FaidxFileHelper faidx, int resolution,
                                            int peakNum,
                                            double[] muFileValues,
                                            List<String> extraFileNames, Map<String, double[]> extraWigFileValues,
                                            int targetIsize, int targetIsizeSD) throws IOException {
        boolean usingMuValues = (muFileValues != null);

        boolean inPositivePeak = false;
        long peakStart = 0;
        int idx = 0;
        double peakMax = 0;

        double muValSum = 0;
        double muValMin = 0;
        double muValMax = 0;

        Map<String, Double> extraWigValueSums = new HashMap<String, Double>();
        Map<String, Double> extraWigValueMins = new HashMap<String, Double>();
        Map<String, Double> extraWigValueMaxes = new HashMap<String, Double>();
        for (String extraWigFile : extraWigFileValues.keySet()) {
            extraWigValueSums.put(extraWigFile, (double) 0);
        }

        while (idx < filteredVals.length) {
            long pos = idx * resolution;

            // If we are filtering regions based on the estimated mean of the second component,
            // if the mean changes more by more than twice the SD of the library we break up the
            // prediction
            if (filteredVals[idx] > 0 &&
                    (! usingMuValues ||
                    (idx < 2 || Math.abs(muFileValues[idx] - muFileValues[idx - 2]) < 2 * targetIsizeSD))) {
                if (!inPositivePeak) {
                    peakStart = pos;
                    inPositivePeak = true;
                    if (usingMuValues) {
                        muValSum = 0;
                        muValMin = Double.POSITIVE_INFINITY;
                        muValMax = Double.NEGATIVE_INFINITY;
                    }
                    for (String extraWigFile : extraWigFileValues.keySet()) {
                        extraWigValueSums.put(extraWigFile, (double) 0);
                        extraWigValueMins.clear();
                        extraWigValueMaxes.clear();
                    }
                }
                peakMax = Math.max(peakMax, filteredVals[idx]);
                if (usingMuValues) {
                    muValSum += muFileValues[idx];
                    muValMin = Math.min(muValMin, muFileValues[idx]);
                    muValMax = Math.max(muValMax, muFileValues[idx]);
                }
                for (String extraWigFile : extraWigFileValues.keySet()) {
                    extraWigValueSums.put(extraWigFile, extraWigValueSums.get(extraWigFile) + extraWigFileValues.get(extraWigFile)[idx]);
                    if (extraWigValueMins.containsKey(extraWigFile)) {
                        extraWigValueMins.put(extraWigFile, Math.min(extraWigValueMins.get(extraWigFile), extraWigFileValues.get(extraWigFile)[idx]));
                    } else {
                        extraWigValueMins.put(extraWigFile, extraWigFileValues.get(extraWigFile)[idx]);
                    }
                    if (extraWigValueMaxes.containsKey(extraWigFile)) {
                        extraWigValueMaxes.put(extraWigFile, Math.max(extraWigValueMaxes.get(extraWigFile), extraWigFileValues.get(extraWigFile)[idx]));
                    } else {
                        extraWigValueMaxes.put(extraWigFile, extraWigFileValues.get(extraWigFile)[idx]);
                    }
                }

            } else {
                if (inPositivePeak) {
                    long endPosition = pos - 1;
                    long length = endPosition - peakStart;
                    double avgMu = muValSum * resolution / ((endPosition + 1) - peakStart);
                    peakNum = determineVariantTypeAndWriteLine(bedFileWriter, currentChromosome, resolution, peakNum, extraFileNames,
                            targetIsize, usingMuValues, peakStart, peakMax, muValMin, muValMax, extraWigValueSums, extraWigValueMins,
                            extraWigValueMaxes, endPosition, length, avgMu);
                    inPositivePeak = false;
                    peakMax = 0;
                }
            }
            idx = idx + 1;
        }
        if (inPositivePeak) {
            long endPosition = faidx.getLengthForChromName(currentChromosome) - 1;
            if (endPosition < peakStart) return peakNum;
            long length = endPosition - peakStart;
            double avgMu = muValSum * resolution / ((endPosition + 1) - peakStart);
            peakNum = determineVariantTypeAndWriteLine(bedFileWriter, currentChromosome, resolution, peakNum, extraFileNames,
                    targetIsize, usingMuValues, peakStart, peakMax, muValMin, muValMax, extraWigValueSums, extraWigValueMins,
                    extraWigValueMaxes, endPosition, length, avgMu);
        }
        return peakNum;
    }

    private static int determineVariantTypeAndWriteLine(BufferedWriter bedFileWriter, String currentChromosome, int resolution, int peakNum,
                                                        List<String> extraFileNames, int targetIsize, boolean usingMuValues, long peakStart,
                                                        double peakMax, double muValMin, double muValMax, Map<String, Double> extraWigValueSums,
                                                        Map<String, Double> extraWigValueMins, Map<String, Double> extraWigValueMaxes,
                                                        long endPosition, long length, double avgMu) throws IOException {
        String variantType = null;
        if (! usingMuValues) {
            variantType = "NA";
        } else if (validDeletionPrediction(targetIsize, length, avgMu)) {
            variantType = "DEL";
        } else if (validInsertionPrediction(targetIsize, avgMu)) {
            variantType = "INS";
        }
        if (variantType != null) {
            writeLine(bedFileWriter, currentChromosome, resolution, peakNum, extraFileNames, peakStart, peakMax,
                    usingMuValues, muValMin, muValMax,
                    extraWigValueSums, extraWigValueMins, extraWigValueMaxes, endPosition, avgMu, variantType);
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
        return (avgMu > targetIsize && Math.abs(predictedRegionLength - avgMu) <= targetIsize);
    }

    /**
     * insertions are only valid if:
     * the estimated mean of the second component is smaller than the target insert size and
     */
    private static boolean validInsertionPrediction(int targetIsize, double avgMu) {
        return (avgMu < targetIsize);
    }

    private static void writeLine(BufferedWriter bedFileWriter, String currentChromosome, int resolution, int peakNum, List<String> extraFileNames, long peakStart,
                                  double peakMax, boolean usingMuValues, double muValMin, double muValMax,
                                  Map<String, Double> extraWigValueSums, Map<String, Double> extraWigValueMins, Map<String, Double> extraWigValueMaxes, long endPosition, double avgMu,
                                  String variantType)
            throws IOException {
        bedFileWriter.write(currentChromosome + "\t" + peakStart + "\t" + endPosition + "\t" + peakNum + "\t" + peakMax);
        if (variantType != null) {
            bedFileWriter.write("\t" + variantType);
        }
        if (usingMuValues) {
            bedFileWriter.write("\t" + avgMu);
            bedFileWriter.write("\t" + muValMin);
            bedFileWriter.write("\t" + muValMax);
        }
        for (String extraWigFile : extraFileNames) {
            bedFileWriter.write("\t" + extraWigValueSums.get(extraWigFile) * resolution / ((endPosition + 1) - peakStart));
            bedFileWriter.write("\t" + extraWigValueMins.get(extraWigFile));
            bedFileWriter.write("\t" + extraWigValueMaxes.get(extraWigFile));
        }

        bedFileWriter.write("\n");
    }


}
