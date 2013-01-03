package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
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
 * Date: 6/5/11
 * Time: 2:34 PM
 */
public class SingleEndAlignmentsToPairsReducer extends MapReduceBase
        implements Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        List<String> read1Alignments = new ArrayList<String>();
        List<String> read2Alignments = new ArrayList<String>();
        while (values.hasNext()) {
            String alignment = values.next().toString();
            char readNum = alignment.charAt(alignment.indexOf("\t") - 1);
            if (readNum == '1') {
                read1Alignments.add(alignment);
            } else if (readNum == '2') {
                read2Alignments.add(alignment);
            } else {
                throw new RuntimeException("bad line: " + alignment);
            }
        }

        if (read1Alignments.size() > 0 || read2Alignments.size() > 0) {
            StringBuffer valueBuffer = new StringBuffer();
            appendAligmentsToBuffer(read1Alignments, valueBuffer);
            valueBuffer.append(Cloudbreak.READ_SEPARATOR);
            appendAligmentsToBuffer(read2Alignments, valueBuffer);
            output.collect(key, new Text(valueBuffer.toString()));
        }
    }

    private void appendAligmentsToBuffer(List<String> read1Alignments, StringBuffer valueBuffer) {
        boolean firstAlignment = true;
        for (String alignment : read1Alignments) {
                if (! firstAlignment) {
                    valueBuffer.append(Cloudbreak.ALIGNMENT_SEPARATOR);
                } else {
                    firstAlignment = false;
                }
                valueBuffer.append(alignment);
        }
    }
}
