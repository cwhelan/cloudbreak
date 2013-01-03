package edu.ohsu.sonmezsysbio.cloudbreak.partitioner;

import edu.ohsu.sonmezsysbio.cloudbreak.CloudbreakMapReduceBase;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocation;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/22/12
 * Time: 10:01 PM
 */
public class GenomicLocationPartitioner extends CloudbreakMapReduceBase implements Partitioner<GenomicLocation, ReadPairInfo> {

    public int getPartition(GenomicLocation key, ReadPairInfo value, int numPartitions) {
        return (key.pos / resolution) % numPartitions;
    }
}
