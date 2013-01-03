package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.CloudbreakMapReduceBase;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 11/6/12
 * Time: 9:42 AM
 */
public class AlignmentKeyMapper extends CloudbreakMapReduceBase implements Mapper<Text, Text, Text, Text> {

    String readId = null;

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        readId = job.get("findalignment.read");
    }

    public void map(Text key, Text value, OutputCollector<Text, Text> textTextOutputCollector, Reporter reporter) throws IOException {
        if (key.toString().contains(readId)) {
            textTextOutputCollector.collect(key, value);
        }
    }
}
