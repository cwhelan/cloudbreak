package edu.ohsu.sonmezsysbio.cloudbreak.io;

import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.ReadPairAlignments;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/8/12
 * Time: 2:42 PM
 */
public abstract class BaseAlignmentReader implements AlignmentReader {

    public ReadPairAlignments parsePairAlignmentLine(String line) {
        return parsePairAlignmentLine(line, null);
    }

    public ReadPairAlignments parsePairAlignmentLine(String line, AlignmentRecordFilter filter) {
        String[] reads = line.split(Cloudbreak.READ_SEPARATOR);
        String read1AlignmentsString = reads[0];
        List<AlignmentRecord> read1AlignmentRecords;
        if (! "".equals(read1AlignmentsString)) {
            String[] read1Alignments = read1AlignmentsString.split(Cloudbreak.ALIGNMENT_SEPARATOR);
            read1AlignmentRecords = parseAlignmentsIntoRecords(read1Alignments, filter);
        } else {
            read1AlignmentRecords = new ArrayList<AlignmentRecord>();
        }

        List<AlignmentRecord> read2AlignmentRecords;
        if (reads.length > 1) {
            String read2AlignmentsString = reads[1];
            String[] read2Alignments = read2AlignmentsString.split(Cloudbreak.ALIGNMENT_SEPARATOR);
            read2AlignmentRecords = parseAlignmentsIntoRecords(read2Alignments, filter);
        } else {
            read2AlignmentRecords = new ArrayList<AlignmentRecord>();
        }
        return new ReadPairAlignments(read1AlignmentRecords, read2AlignmentRecords);
    }

    public List<AlignmentRecord> parseAlignmentsIntoRecords(String[] alignments, AlignmentRecordFilter filter) {
        List<AlignmentRecord> read1AlignmentList = new ArrayList<AlignmentRecord>();
        for (String alignment : alignments) {
            AlignmentRecord record1 = parseRecord(alignment);
            if (filter == null || filter.passes(record1)) {
                read1AlignmentList.add(record1);
            }
        }
        return read1AlignmentList;
    }
}
