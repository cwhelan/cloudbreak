package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import com.google.common.base.Joiner;
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
 * Date: 4/16/12
 * Time: 1:46 PM
 *
 * This class reduces the alignment counts and outputs the following totals:
 * number of reads
 * number of alignments
 * number of mismatches in the best mappings for each read
 */

public class AlignmentSummaryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Long[] totals = null;
        while (values.hasNext()) {
            Text val = values.next();
            String[] fields = val.toString().split("\t");
            if (totals == null) {
                totals = new Long[fields.length];
                for (int i = 0; i < totals.length; i++) {
                    totals[i] = 0l;
                }
            }
            for (int i = 0; i < fields.length; i++) {
                if (! "".equals(fields[i])) {
                    totals[i] += Long.parseLong(fields[i]);
                }
            }
        }

        String outvals = Joiner.on("\t").join(totals);
        output.collect(key, new Text(outvals));
    }

}
