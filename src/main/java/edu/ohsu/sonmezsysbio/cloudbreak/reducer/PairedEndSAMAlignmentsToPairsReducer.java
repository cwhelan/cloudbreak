package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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
public class PairedEndSAMAlignmentsToPairsReducer extends MapReduceBase
        implements Reducer<Text,Text,Text,Text> {

    private static Logger logger = Logger.getLogger(PairedEndSAMAlignmentsToPairsReducer.class);

    // { logger.setLevel(Level.DEBUG); }

    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        logger.debug("reducing for key " + key);
        List<String> read1Alignments = new ArrayList<String>();
        List<String> read2Alignments = new ArrayList<String>();
        while (values.hasNext()) {
            String alignment = values.next().toString();
            if (logger.isDebugEnabled()) {
                logger.debug("got alignment " + alignment);
            }
            String[] fields = alignment.split("\t");
            int flag = Integer.parseInt(fields[1]);

            if ((flag & 0x40) == 0x40) {
                logger.debug("adding to read1 alignments");
                read1Alignments.add(alignment);
            } else {
                logger.debug("adding to read2 alignments");
                read2Alignments.add(alignment);
            }
        }

        if (read1Alignments.size() > 0 && read2Alignments.size() > 0) {
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
