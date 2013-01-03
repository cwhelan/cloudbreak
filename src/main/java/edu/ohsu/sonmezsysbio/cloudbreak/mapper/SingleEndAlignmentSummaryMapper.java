package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/16/12
 * Time: 1:44 PM
 */
public class SingleEndAlignmentSummaryMapper extends CloudbreakMapReduceBase implements Mapper<Text, Text, Text, Text> {

    Text outKey = new Text("k");

    public void map(Text key, Text value, OutputCollector output, Reporter reporter) throws IOException {
        String line = value.toString();
        ReadPairAlignments readPairAlignments = alignmentReader.parsePairAlignmentLine(line);

        summarizeAlignments(output, readPairAlignments.getRead1Alignments());
        summarizeAlignments(output, readPairAlignments.getRead2Alignments());

    }

    private void summarizeAlignments(OutputCollector output, List<AlignmentRecord> aligments) throws IOException {
        String vals = "1\t" + (aligments.size());
        if (aligments.size() > 0) {
            int bestMismatches = Integer.MAX_VALUE;
            if (Cloudbreak.ALIGNER_GENERIC_SAM.equals(getAlignerName())) {
                for (AlignmentRecord alignmentRecord : aligments) {
                    SAMRecord samRecord = (SAMRecord) alignmentRecord;
                    if (samRecord.getMismatches() < bestMismatches) {
                        bestMismatches = samRecord.getMismatches();
                    }
                }
                vals = vals + "\t" + bestMismatches;
            }
            output.collect(outKey, new Text(vals));
        }
    }

}