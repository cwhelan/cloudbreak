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

/**
 * Reads alignment records in SAM Format.
 */
public class SAMAlignmentReader extends BaseAlignmentReader {

    public AlignmentRecord parseRecord(String alignmentRecord) {
        return parseRecord(alignmentRecord.split("\t"));
    }

    public AlignmentRecord parseRecord(String[] fields) {
        return SAMRecord.parseSamRecord(fields);
    }

    public double probabilityMappingIsCorrect(AlignmentRecord record1, AlignmentRecord record2, ReadPairAlignments readPairAlignments) {
        double r1normalization = readPairAlignments.sumMismatchScores1();
        double r2normalization = readPairAlignments.sumMismatchScores2();
        return Math.log(record1.mismatchScore() / r1normalization) + Math.log(record2.mismatchScore() / r2normalization);
    }


}
