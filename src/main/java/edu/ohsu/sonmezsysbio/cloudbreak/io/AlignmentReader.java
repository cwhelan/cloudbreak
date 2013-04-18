package edu.ohsu.sonmezsysbio.cloudbreak.io;

import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.ReadPairAlignments;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/8/12
 * Time: 2:05 PM
 */

/**
 * Generic interface for an alignment parser depending on the alignment record format.
 */
public interface AlignmentReader {
    AlignmentRecord parseRecord(String alignmentRecord);

    double probabilityMappingIsCorrect(AlignmentRecord record1, AlignmentRecord record2, ReadPairAlignments readPairAlignments);

    ReadPairAlignments parsePairAlignmentLine(String line);

    ReadPairAlignments parsePairAlignmentLine(String line, AlignmentRecordFilter filter);

    void setLegacyAlignments(boolean legacyAlignments);

    boolean isLegacyAlignments();

    boolean isStripChromosomeNameAtWhitespace();

    void setStripChromosomeNameAtWhitespace(boolean stripChromosomeNameAtWhitespace);

    /**
     * Factory method to grab the correct implementing class.
     */
    public static class AlignmentReaderFactory {
        public static AlignmentReader getInstance(String aligner) {
            if (Cloudbreak.ALIGNER_NOVOALIGN.equals(aligner)) return new NovoalignAlignmentReader();
            if (Cloudbreak.ALIGNER_MRFAST.equals(aligner)) return new MrfastAlignmentReader();
            if (Cloudbreak.ALIGNER_GENERIC_SAM.equals(aligner)) return new SAMAlignmentReader();
            return null;
        }

    }
}
