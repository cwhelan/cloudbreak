package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.CloudbreakMapReduceBase;
import edu.ohsu.sonmezsysbio.svpipeline.io.GenomicLocation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 7/18/12
 * Time: 11:39 AM
 */
public class GenomicLocationScoreThresholdMapper extends CloudbreakMapReduceBase implements Mapper<GenomicLocation, DoubleWritable, GenomicLocation, DoubleWritable> {
    private double threshold;

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        if (job.get("cloudbreak.threshold") != null) {
            threshold = Double.parseDouble(job.get("cloudbreak.threshold"));
        }

    }

    public void map(GenomicLocation key, DoubleWritable value, OutputCollector<GenomicLocation, DoubleWritable> output, Reporter reporter) throws IOException {
        if (value.get() > threshold) {
            output.collect(key, value);
        }
    }
}
