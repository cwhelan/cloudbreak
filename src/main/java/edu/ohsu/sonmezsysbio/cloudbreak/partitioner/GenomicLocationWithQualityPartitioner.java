package edu.ohsu.sonmezsysbio.cloudbreak.partitioner;

import edu.ohsu.sonmezsysbio.cloudbreak.CloudbreakMapReduceBase;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocationWithQuality;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/22/12
 * Time: 12:03 PM
 */
public class GenomicLocationWithQualityPartitioner extends CloudbreakMapReduceBase implements Partitioner<GenomicLocationWithQuality, ReadPairInfo> {
    public int getPartition(GenomicLocationWithQuality key, ReadPairInfo value, int numPartitions) {
        return (key.pos / resolution) % numPartitions;
    }

}
