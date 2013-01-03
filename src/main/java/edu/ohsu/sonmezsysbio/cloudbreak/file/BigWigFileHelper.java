package edu.ohsu.sonmezsysbio.cloudbreak.file;

import org.broad.igv.bbfile.BBFileReader;
import org.broad.igv.bbfile.BigWigIterator;
import org.broad.igv.bbfile.WigItem;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/25/12
 * Time: 1:55 PM
 */
public class BigWigFileHelper {


    private BBFileReader reader;

    public void open(String path) throws IOException {
        reader = new BBFileReader(path);
    }

    public double getAverageValueForRegion(String chromosome, int start, int end) throws IOException {
        BigWigIterator iterator = reader.getBigWigIterator(chromosome, start, chromosome, end, false);
        double valueSum = 0;
        int basesCovered = 0;
        while (iterator.hasNext()) {
            WigItem item = iterator.next();
            int itemBasesCovered = Math.min(end, item.getEndBase()) - Math.max(item.getStartBase(), start);
            valueSum += item.getWigValue() * itemBasesCovered;
            basesCovered += itemBasesCovered;
        }
        return valueSum / basesCovered;
    }

    public double getMinValueForRegion(String chromosome, int start, int end) throws IOException {
        BigWigIterator iterator = reader.getBigWigIterator(chromosome, start, chromosome, end, false);
        double lowestValue = Double.MAX_VALUE;
        while (iterator.hasNext()) {
            WigItem item = iterator.next();
            if (item.getWigValue() < lowestValue) lowestValue = item.getWigValue();
        }
        return lowestValue;
    }

    public void close() throws IOException {
        reader.getBBFis().close();
    }

}
