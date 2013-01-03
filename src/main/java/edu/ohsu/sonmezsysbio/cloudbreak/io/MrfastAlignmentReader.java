package edu.ohsu.sonmezsysbio.cloudbreak.io;

import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.MrfastAlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.ReadPairAlignments;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/8/12
 * Time: 2:43 PM
 */
public class MrfastAlignmentReader extends BaseAlignmentReader {

    private Double read1AlignmentNormalization;
    private Double read2AlignmentNormalization;

    public AlignmentRecord parseRecord(String alignmentRecord) {
        return parseRecord(alignmentRecord.split("\t"));
    }

    public AlignmentRecord parseRecord(String[] fields) {
        MrfastAlignmentRecord record = new MrfastAlignmentRecord();

        // return readId + "\t" + orientation + "\t" + chrom + "\t" + position + "\t" + nm + "\t" + sequenceLength;
        record.setReadId(fields[0]);
        record.setForward("F".equals(fields[1]));
        record.setChromosomeName(parseChromosome(fields[2]));
        record.setPosition(Integer.parseInt(fields[3]));
        record.setMismatches(Integer.parseInt(fields[4]));
        record.setSequenceLength(Integer.parseInt(fields[5]));
        return record;

    }

    String parseChromosome(String field) {
        return field.substring(0, field.indexOf(" "));
    }

    public double probabilityMappingIsCorrect(AlignmentRecord record1, AlignmentRecord record2, ReadPairAlignments readPairAlignments) {
        double record1Score = alignmentScore(record1) / computeAlignmentNormalization(readPairAlignments.getRead1Alignments());
        double record2Score = alignmentScore(record2) / computeAlignmentNormalization(readPairAlignments.getRead2Alignments());
        return Math.log(record1Score) + Math.log(record2Score);
    }

    static Double computeAlignmentNormalization(List<AlignmentRecord> readAlignments) {
        double totalScore = 0;
        for (AlignmentRecord alignment : readAlignments) {
            double score = alignmentScore(alignment);
            totalScore += score;
        }
        return totalScore;
    }

    public static double alignmentScore(AlignmentRecord alignment) {
        MrfastAlignmentRecord mrfastAlignment = (MrfastAlignmentRecord) alignment;
        int length = mrfastAlignment.getSequenceLength();
        int mismatches = mrfastAlignment.getMismatches();
        return Math.pow(length - mismatches, 1 / (mismatches + 1));
    }

}
