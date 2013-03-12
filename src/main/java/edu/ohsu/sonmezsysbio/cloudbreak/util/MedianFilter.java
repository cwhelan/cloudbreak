package edu.ohsu.sonmezsysbio.cloudbreak.util;

import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/11/13
 * Time: 10:12 PM
 */
public class MedianFilter {
    public static double[] medianFilterValues(double[] values, int medianFilterWindow, double threshold) {
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
}
