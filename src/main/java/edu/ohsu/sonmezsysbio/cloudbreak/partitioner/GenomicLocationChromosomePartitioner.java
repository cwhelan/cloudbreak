package edu.ohsu.sonmezsysbio.cloudbreak.partitioner;

import edu.ohsu.sonmezsysbio.cloudbreak.CloudbreakMapReduceBase;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 7/6/13
 * Time: 2:39 PM
 */
public class GenomicLocationChromosomePartitioner extends CloudbreakMapReduceBase implements Partitioner<GenomicLocation, Text> {
    public int getPartition(GenomicLocation key, Text value, int numPartitions) {
        return key.chromosome;
    }
}
