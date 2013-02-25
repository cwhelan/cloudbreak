package edu.ohsu.sonmezsysbio.cloudbreak.io;

import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.ReadPairAlignments;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/8/12
 * Time: 2:42 PM
 */

/**
 * Contains common logic for parsing alignment records. Alignments are stored in the form:
 *
 * R1alignment1  ALIGNMENT_SEPARATOR R1alignment2  ALIGNMENT_SEPARATOR    ... READ_SEPARATOR R2alignment1    ALIGNMENT_SEPARATOR    R2alignment2    ...
 *
 * Where R1alignment1 is the first alignment for read 1 in the pair, etc.
 *
 */
public abstract class BaseAlignmentReader implements AlignmentReader {

    boolean legacyAlignments;

    public boolean isLegacyAlignments() {
        return legacyAlignments;
    }

    public void setLegacyAlignments(boolean legacyAlignments) {
        this.legacyAlignments = legacyAlignments;
    }

    public ReadPairAlignments parsePairAlignmentLine(String line) {
        return parsePairAlignmentLine(line, null);
    }

    public ReadPairAlignments parsePairAlignmentLine(String line, AlignmentRecordFilter filter) {
        String[] reads = line.split(getReadSeparator());
        String read1AlignmentsString = reads[0];
        List<AlignmentRecord> read1AlignmentRecords;
        if (! "".equals(read1AlignmentsString)) {
            String[] read1Alignments = read1AlignmentsString.split(getAlignmentSeparator());
            read1AlignmentRecords = parseAlignmentsIntoRecords(read1Alignments, filter);
        } else {
            read1AlignmentRecords = new ArrayList<AlignmentRecord>();
        }

        List<AlignmentRecord> read2AlignmentRecords;
        if (reads.length > 1) {
            String read2AlignmentsString = reads[1];
            String[] read2Alignments = read2AlignmentsString.split(getAlignmentSeparator());
            read2AlignmentRecords = parseAlignmentsIntoRecords(read2Alignments, filter);
        } else {
            read2AlignmentRecords = new ArrayList<AlignmentRecord>();
        }
        return new ReadPairAlignments(read1AlignmentRecords, read2AlignmentRecords);
    }

    private String getAlignmentSeparator() {
        if (isLegacyAlignments()) {
            return Cloudbreak.LEGACY_ALIGNMENT_SEPARATOR;
        } else {
            return Cloudbreak.ALIGNMENT_SEPARATOR;
        }
    }

    private String getReadSeparator() {
        if (isLegacyAlignments()) {
            return Cloudbreak.LEGACY_READ_SEPARATOR;
        } else {
            return Cloudbreak.READ_SEPARATOR;
        }
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
