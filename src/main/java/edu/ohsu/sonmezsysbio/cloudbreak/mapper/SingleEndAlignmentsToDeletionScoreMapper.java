package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/23/11
 * Time: 10:12 AM
 */
public class SingleEndAlignmentsToDeletionScoreMapper extends CloudbreakMapReduceBase implements Mapper<Text, Text, Text, DoubleWritable> {

    private boolean matePairs;
    private Integer maxInsertSize = 500000;
    private PairedAlignmentScorer scorer;

    public Integer getMaxInsertSize() {
        return maxInsertSize;
    }

    public void setMaxInsertSize(Integer maxInsertSize) {
        this.maxInsertSize = maxInsertSize;
    }

    public Double getTargetIsize() {
        return targetIsize;
    }

    public void setTargetIsize(Double targetIsize) {
        this.targetIsize = targetIsize;
    }

    public Double getTargetIsizeSD() {
        return targetIsizeSD;
    }

    public void setTargetIsizeSD(Double targetIsizeSD) {
        this.targetIsizeSD = targetIsizeSD;
    }

    public boolean isMatePairs() {
        return matePairs;
    }

    public void setMatePairs(boolean matePairs) {
        this.matePairs = matePairs;
    }

    public PairedAlignmentScorer getScorer() {
        return scorer;
    }

    public void setScorer(PairedAlignmentScorer scorer) {
        this.scorer = scorer;
    }

    private Double targetIsize;
    private Double targetIsizeSD;

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        targetIsize = Double.parseDouble(job.get("pileupDeletionScore.targetIsize"));
        targetIsizeSD = Double.parseDouble(job.get("pileupDeletionScore.targetIsizeSD"));

        maxInsertSize = Integer.parseInt(job.get("pileupDeletionScore.maxInsertSize"));
        matePairs = Boolean.parseBoolean(job.get("pileupDeletionScore.isMatePairs"));

        scorer = new ProbabilisticPairedAlignmentScorer();

    }

    public void map(Text key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException {
        String line = value.toString();

        ReadPairAlignments readPairAlignments = alignmentReader.parsePairAlignmentLine(line);
        // ignoring OEA for now
        if (readPairAlignments.getRead1Alignments().size() == 0 || readPairAlignments.getRead2Alignments().size() == 0) {
            return;
        }

        emitDeletionScoresForAllPairs(readPairAlignments, output);
    }

    private void emitDeletionScoresForAllPairs(ReadPairAlignments readPairAlignments, OutputCollector<Text, DoubleWritable> output) throws IOException {
        for (AlignmentRecord record1 : readPairAlignments.getRead1Alignments()) {
            for (AlignmentRecord record2 : readPairAlignments.getRead2Alignments()) {
                emitDeletionScoresForPair(record1, record2, readPairAlignments, output);
            }
        }
    }

    private void emitDeletionScoresForPair(AlignmentRecord record1, AlignmentRecord record2, ReadPairAlignments readPairAlignments, OutputCollector<Text, DoubleWritable> output) throws IOException {

        // todo: not handling translocations for now
        if (! record1.getChromosomeName().equals(record2.getChromosomeName())) return;

        int insertSize = -1;
        Double isizeMean;
        Double isizeSD;

        AlignmentRecord leftRead = record1.getPosition() < record2.getPosition() ?
                record1 : record2;
        AlignmentRecord rightRead = record1.getPosition() < record2.getPosition() ?
                record2 : record1;

        // todo: not handling inversions for now
        if (!scorer.validateMappingOrientations(record1, record2, matePairs)) return;

        isizeMean = targetIsize;
        isizeSD = targetIsizeSD;
        if (matePairs) {
            if (!scorer.isMatePairNotSmallFragment(record1, record2)) {
                isizeMean = 150.0;
                isizeSD = 15.0;
            }
        }

        insertSize = rightRead.getPosition() + rightRead.getSequenceLength() - leftRead.getPosition();

        if (! scorer.validateInsertSize(insertSize, record1.getReadId(), maxInsertSize)) return;

        Double pMappingCorrect = alignmentReader.probabilityMappingIsCorrect(record1, record2, readPairAlignments);

        double deletionScore = scorer.computeDeletionScore(
                insertSize,
                isizeMean,
                isizeSD,
                pMappingCorrect
        );

        int genomeOffset = leftRead.getPosition() - leftRead.getPosition() % resolution;

        insertSize = insertSize + leftRead.getPosition() % resolution + resolution - rightRead.getPosition() % resolution;

        for (int i = 0; i <= insertSize; i = i + resolution) {
            Text outKey = new Text(record1.getChromosomeName() + "\t" + (genomeOffset + i));
            DoubleWritable outVal = new DoubleWritable(deletionScore);
            output.collect(outKey, outVal);
        }

    }

}
