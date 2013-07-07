package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.CloudbreakMapReduceBase;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocation;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GMMScorerResults;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/11/13
 * Time: 9:56 PM
 */
public class GMMResultsToChromosomeMapper extends CloudbreakMapReduceBase implements Mapper<GenomicLocation,
        GMMScorerResults, GenomicLocation, Text> {
    @Override
    public void map(GenomicLocation genomicLocation, GMMScorerResults gmmScorerResults,
                    OutputCollector<GenomicLocation, Text> intWritableTextOutputCollector, Reporter reporter) throws IOException {
        intWritableTextOutputCollector.collect(genomicLocation,
                new Text(genomicLocation.pos + "\t" + gmmScorerResults.lrHeterozygous + "\t" + gmmScorerResults.mu2 + "\t" + gmmScorerResults.w0));
    }
}
