package edu.ohsu.sonmezsysbio.cloudbreak.util;

import edu.ohsu.sonmezsysbio.cloudbreak.reducer.FilteredValueSet;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GMMResultsToVariantCallsReducer;

import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/11/13
 * Time: 10:12 PM
 */
public class MedianFilter {
    public static FilteredValueSet[] medianFilterValues(GMMResultsToVariantCallsReducer.GMMResultsIterator iterator, int medianFilterWindow, double threshold, Long chromLength, int resolution) {
        FilteredValueSet[] list = new FilteredValueSet[3];
        FilteredValueSet lrValues = new FilteredValueSet();
        FilteredValueSet muValues = new FilteredValueSet();
        FilteredValueSet w0Values = new FilteredValueSet();
        int idx = 0;

        double[] window = new double[medianFilterWindow];
        double[] muWindow = new double[medianFilterWindow];
        double[] w0Window = new double[medianFilterWindow];

        int maxWindowIdx = (int) (chromLength / resolution);
        lrValues.maxIndex = maxWindowIdx;
        w0Values.maxIndex = maxWindowIdx;
        muValues.maxIndex = maxWindowIdx;

        int middleOfWindow = medianFilterWindow / 2;
        for (idx = 0; idx <= maxWindowIdx; idx++) {
            long pos = idx * resolution;
            GMMResultsToVariantCallsReducer.GMMResult result = iterator.getNextResult(pos);
            System.arraycopy(window, 0, window, 1, medianFilterWindow - 1);
            System.arraycopy(muWindow, 0, muWindow, 1, medianFilterWindow - 1);
            System.arraycopy(w0Window, 0, w0Window, 1, medianFilterWindow - 1);
            window[0] = result.lrVal;
            muWindow[0] = result.muval;
            w0Window[0] = result.w0val;

            if (idx < middleOfWindow) {
                if (window[0] > threshold) {
                    lrValues.putVal(idx, window[0]);
                    muValues.putVal(idx, muWindow[0]);
                    w0Values.putVal(idx, w0Window[0]);
                }
            } else if (idx >= medianFilterWindow - 1) {
                double[] sortedWindow = Arrays.copyOf(window, medianFilterWindow);
                Arrays.sort(sortedWindow);
                double medianValue = sortedWindow[middleOfWindow];
                if (medianValue > threshold) {
                    putValues(lrValues, muValues, w0Values, idx - middleOfWindow, window[middleOfWindow], muWindow, w0Window, medianFilterWindow, middleOfWindow);
                }
            }
        }
        for (int i = middleOfWindow - 1; i >= 0; i--) {
            if (window[i] > threshold) {
                putValues(lrValues, muValues, w0Values, idx, window[i], muWindow, w0Window, medianFilterWindow, i);
            }
            idx++;
        }

        list[0] = lrValues;
        list[1] = muValues;
        list[2] = w0Values;
        return list;
    }

    private static void putValues(FilteredValueSet lrValues, FilteredValueSet muValues, FilteredValueSet w0Values, int idx, double val,
                                  double[] muWindow, double[] w0Window, int medianFilterWindow,
                                  int positionInWindow) {
        int targetSpot = idx;
        lrValues.putVal(targetSpot, val);
        muValues.putVal(targetSpot, muWindow[positionInWindow]);
        w0Values.putVal(targetSpot, w0Window[positionInWindow]);

        // put in the surrounding mu values
        if (positionInWindow + 1 < medianFilterWindow) {

            muValues.putVal(targetSpot - 1, muWindow[positionInWindow + 1]);
            w0Values.putVal(targetSpot - 1, w0Window[positionInWindow + 1]);
        }
        if (positionInWindow + 2 < medianFilterWindow) {
            muValues.putVal(targetSpot - 2, muWindow[positionInWindow + 2]);
            w0Values.putVal(targetSpot - 2, w0Window[positionInWindow + 2]);
        }

        if (positionInWindow - 1 >= 0) {
            muValues.putVal(targetSpot + 1, muWindow[positionInWindow - 1]);
            w0Values.putVal(targetSpot + 1, w0Window[positionInWindow - 1]);
        }
        if (positionInWindow - 2 >= 0) {
            muValues.putVal(targetSpot + 2, muWindow[positionInWindow - 2]);
            w0Values.putVal(targetSpot + 2, w0Window[positionInWindow - 2]);
        }
    }
}
