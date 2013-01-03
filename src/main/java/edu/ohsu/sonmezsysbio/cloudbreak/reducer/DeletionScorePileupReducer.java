package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/23/11
 * Time: 10:15 AM
 */
public class DeletionScorePileupReducer extends MapReduceBase implements Reducer<Text,DoubleWritable,Text,DoubleWritable>{
    public void reduce(Text key, Iterator<DoubleWritable> values,
                       OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        double sum = 0;
        while (values.hasNext()) {
            DoubleWritable value = values.next();
            sum += value.get();
        }
        output.collect(key, new DoubleWritable(sum));
    }
}
