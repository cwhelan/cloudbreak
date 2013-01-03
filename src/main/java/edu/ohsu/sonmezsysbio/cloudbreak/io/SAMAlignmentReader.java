package edu.ohsu.sonmezsysbio.cloudbreak.io;

import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.ReadPairAlignments;
import edu.ohsu.sonmezsysbio.cloudbreak.SAMRecord;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 10/15/12
 * Time: 1:36 PM
 */
public class SAMAlignmentReader extends BaseAlignmentReader {

    public AlignmentRecord parseRecord(String alignmentRecord) {
        return parseRecord(alignmentRecord.split("\t"));
    }

    public AlignmentRecord parseRecord(String[] fields) {
        return SAMRecord.parseSamRecord(fields);
    }

    public double probabilityMappingIsCorrect(AlignmentRecord record1, AlignmentRecord record2, ReadPairAlignments readPairAlignments) {
        double r1normalization = sumMismatchScores(readPairAlignments.getRead1Alignments());
        double r2normalization = sumMismatchScores(readPairAlignments.getRead2Alignments());
        return Math.log(mismatchScore(record1) / r1normalization) + Math.log(mismatchScore(record2) / r2normalization);
    }

    private double sumMismatchScores(List<AlignmentRecord> alignments) {
        double sumAlignmentScores = 0;
        for (AlignmentRecord record : alignments) {
            sumAlignmentScores += mismatchScore(record);
        }
        return sumAlignmentScores;
    }

    private double mismatchScore(AlignmentRecord record) {
        SAMRecord samRecord = (SAMRecord) record;
        int mismatches = samRecord.getMismatches();
        return Math.exp( -1 * mismatches / 2);

    }

}
