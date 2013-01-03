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
public class WigFileHelper {

    private static org.apache.log4j.Logger logger = Logger.getLogger(WigFileHelper.class);

    public static void averageWigOverSlidingWindow(int resolution, int windowSizeToAverageOver, BufferedReader inFileReader, BufferedWriter outFileWriter) throws IOException {

            String line;

            HashMap<Integer,Double> window;
            double windowTotal;
            int lastPos;

            window = new HashMap<Integer, Double>();
            windowTotal = 0;
            lastPos = 0;

            while ((line = inFileReader.readLine()) != null) {
                if (line.startsWith("track")) {
                    outFileWriter.write(line + "\n");
                    continue;
                }

                if (line.startsWith("variableStep")) {
                    outFileWriter.write(line + "\n");

                    window = new HashMap<Integer, Double>();
                    windowTotal = 0;
                    lastPos = 0;

                    continue;
                }

                String[] fields = line.split("\t");
                Integer pos = Integer.parseInt(fields[0]);
                if (pos - lastPos > resolution) {
                    logger.info("hit a gap between " + lastPos + " and " + pos);
                }
                while (pos - lastPos > resolution) {
                    lastPos = lastPos + resolution;
                    logger.debug("adding "+ lastPos + ", " + 0);
                    window.put(lastPos,0d);
                    if (window.keySet().size() > windowSizeToAverageOver) {
                        windowTotal = writeVal(outFileWriter, window, windowTotal, lastPos, windowSizeToAverageOver, resolution);
                    }
                }
                lastPos = pos;
                Double val = Double.parseDouble(fields[1]);
                logger.debug("adding "+ pos + ", " + val);
                window.put(pos,val);
                windowTotal += val;


                if (window.keySet().size() > windowSizeToAverageOver) {
                    windowTotal = writeVal(outFileWriter, window, windowTotal, pos, windowSizeToAverageOver, resolution);
                }
            }
    }

    private static double writeVal(BufferedWriter writer, HashMap<Integer, Double> window, double windowTotal, Integer pos, int windowToAverageOver, int resolution) throws IOException {
        Double avg = windowTotal / windowToAverageOver;

        if (! window.containsKey(pos -  (windowToAverageOver / 2) * resolution)) {
            logger.warn("Current position = " + pos + ", but did not have mid position " + (pos - (windowToAverageOver / 2) * resolution));
        }
        Double midVal = window.get(pos - (windowToAverageOver / 2) * resolution);

        Double modVal = midVal - avg;

        int positionToLeave = pos - windowToAverageOver * resolution;
        if (! window.containsKey(pos -  windowToAverageOver * resolution)) {
            logger.warn("Current position = " + pos + ", but did not have begin position " + (pos - windowToAverageOver * resolution));
            List<Integer> positions = new ArrayList<Integer>(window.keySet());
            Collections.sort(positions);
            logger.warn("lowest positions = " + positions.get(0) + ", " + positions.get(1));
        }

        writer.write(Integer.toString(pos - (windowToAverageOver / 2) * resolution)  + "\t" + modVal + "\n");


        Double leaving = window.get(positionToLeave);

        windowTotal -= leaving;
        window.remove(positionToLeave);
        return windowTotal;
    }

    public static void exportRegionsOverThresholdFromWig(String name, BufferedReader wigFileReader, BufferedWriter bedFileWriter,
                                                         Double threshold, FaidxFileHelper faidx, int medianFilterWindow
                                                         ) throws IOException {
        exportRegionsOverThresholdFromWig(name, wigFileReader, bedFileWriter, threshold, faidx, medianFilterWindow, null, null,
                new ArrayList<String>(), new HashMap<String, BufferedReader>());

    }

    public static void exportRegionsOverThresholdFromWig(String outputPrefix, BufferedReader wigFileReader,
                                                         BufferedWriter bedFileWriter, double threshold,
                                                         FaidxFileHelper faidx, int medianFilterWindow,
                                                         List<String> extraFileNames, Map<String, BufferedReader> extraWigFileReaders) throws IOException {
        exportRegionsOverThresholdFromWig(outputPrefix, wigFileReader, bedFileWriter, threshold, faidx, medianFilterWindow, null, null,
                extraFileNames, extraWigFileReaders);
    }

    public static void exportRegionsOverThresholdFromWig(String outputPrefix, BufferedReader wigFileReader,
                                                         BufferedWriter bedFileWriter, double threshold,
                                                         FaidxFileHelper faidx, int medianFilterWindow,
                                                         String muFile, BufferedReader muFileReader,
                                                         List<String> extraFileNames, Map<String, BufferedReader> extraWigFileReaders)
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
                            peakNum, muFileValues, extraFileNames, extraWigFileValues);
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
                extraWigFileValues);

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
                                            List<String> extraFileNames, Map<String, double[]> extraWigFileValues) throws IOException {
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

            if (filteredVals[idx] > 0 &&
                    (! usingMuValues ||
                    (idx < 2 || Math.abs(muFileValues[idx] - muFileValues[idx - 2]) < 60))) {
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
                    writeLine(bedFileWriter, currentChromosome, resolution, peakNum, extraFileNames, peakStart, peakMax,
                            usingMuValues, muValSum, muValMin, muValMax,
                            extraWigValueSums, extraWigValueMins, extraWigValueMaxes, endPosition);
                    peakNum += 1;
                    inPositivePeak = false;
                    peakMax = 0;
                }
            }
            idx = idx + 1;
        }
        if (inPositivePeak) {
            long endPosition = faidx.getLengthForChromName(currentChromosome) - 1;
            if (endPosition < peakStart) return peakNum;
            writeLine(bedFileWriter, currentChromosome, resolution, peakNum, extraFileNames, peakStart, peakMax,
                    usingMuValues, muValSum, muValMin, muValMax,
                    extraWigValueSums, extraWigValueMins, extraWigValueMaxes, endPosition);
            peakNum += 1;
        }
        return peakNum;
    }

    private static void writeLine(BufferedWriter bedFileWriter, String currentChromosome, int resolution, int peakNum, List<String> extraFileNames, long peakStart,
                                  double peakMax, boolean usingMuValues, double muValSum, double muValMin, double muValMax,
                                  Map<String, Double> extraWigValueSums, Map<String, Double> extraWigValueMins, Map<String, Double> extraWigValueMaxes, long endPosition)
            throws IOException {
        bedFileWriter.write(currentChromosome + "\t" + peakStart + "\t" + endPosition + "\t" + peakNum + "\t" + peakMax);
        if (usingMuValues) {
            bedFileWriter.write("\t" + muValSum * resolution / ((endPosition + 1) - peakStart));
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
