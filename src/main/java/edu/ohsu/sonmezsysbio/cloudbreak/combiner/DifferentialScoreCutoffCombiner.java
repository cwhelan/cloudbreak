package edu.ohsu.sonmezsysbio.cloudbreak.combiner;

import edu.ohsu.sonmezsysbio.cloudbreak.CloudbreakMapReduceBase;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocationWithQuality;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 12/3/12
 * Time: 4:08 PM
 */
public class DifferentialScoreCutoffCombiner extends CloudbreakMapReduceBase
        implements Reducer<GenomicLocationWithQuality, ReadPairInfo, GenomicLocationWithQuality, ReadPairInfo> {

    private double maxScoreDiff;

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        maxScoreDiff = Double.parseDouble(job.get("max.log.mapq.diff"));
    }

    public void reduce(GenomicLocationWithQuality genomicLocationWithQuality, Iterator<ReadPairInfo> readPairInfoIterator,
                       OutputCollector<GenomicLocationWithQuality, ReadPairInfo> genomicLocationWithQualityReadPairInfoOutputCollector,
                       Reporter reporter) throws IOException {
        double maxScore = Double.NEGATIVE_INFINITY;
        List<ReadPairInfo> rpis = new ArrayList<ReadPairInfo>();
        while (readPairInfoIterator.hasNext()) {
            ReadPairInfo rpi = readPairInfoIterator.next();
            if (rpi.pMappingCorrect > maxScore) {
                maxScore = rpi.pMappingCorrect;
            }
            if (maxScore - rpi.pMappingCorrect < maxScoreDiff) {
                rpis.add(rpi);
            }
        }
        for (ReadPairInfo rpi : rpis) {
            if (maxScore - rpi.pMappingCorrect < maxScoreDiff) {
                genomicLocationWithQualityReadPairInfoOutputCollector.collect(genomicLocationWithQuality, rpi);
            }
        }
    }
}
