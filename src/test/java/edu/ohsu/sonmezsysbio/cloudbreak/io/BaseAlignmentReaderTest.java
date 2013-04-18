package edu.ohsu.sonmezsysbio.cloudbreak.io;

import edu.ohsu.sonmezsysbio.cloudbreak.*;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 11/26/12
 * Time: 10:23 AM
 */
public class BaseAlignmentReaderTest {

    @Test
    public void testParsePairAlignmentLine() throws Exception {
        BaseAlignmentReader reader = new BaseAlignmentReader() {
            public AlignmentRecord parseRecord(String alignmentRecord) {
                NovoalignNativeRecord record = new NovoalignNativeRecord();
                record.setReadId(alignmentRecord);
                return record;
            }

            public double probabilityMappingIsCorrect(AlignmentRecord record1, AlignmentRecord record2, ReadPairAlignments readPairAlignments) {
                return 0;
            }
        };
        String line = "a" + Cloudbreak.READ_SEPARATOR + "foo" + Cloudbreak.ALIGNMENT_SEPARATOR + "bar";
        ReadPairAlignments alignments = reader.parsePairAlignmentLine(line);
        assertEquals(1, alignments.getRead1Alignments().size());
        assertEquals(2, alignments.getRead2Alignments().size());
    }

        @Test
    public void testParsePairAlignmentLine_OEA() throws Exception {
        BaseAlignmentReader reader = new BaseAlignmentReader() {
            public AlignmentRecord parseRecord(String alignmentRecord) {
                NovoalignNativeRecord record = new NovoalignNativeRecord();
                record.setReadId(alignmentRecord);
                return record;
            }

            public double probabilityMappingIsCorrect(AlignmentRecord record1, AlignmentRecord record2, ReadPairAlignments readPairAlignments) {
                return 0;
            }
        };
        String oeaLine = Cloudbreak.READ_SEPARATOR + "foo" + Cloudbreak.ALIGNMENT_SEPARATOR + "bar";
        ReadPairAlignments alignments = reader.parsePairAlignmentLine(oeaLine);
        assertEquals(0, alignments.getRead1Alignments().size());
        assertEquals(2, alignments.getRead2Alignments().size());

        oeaLine = "foo" + Cloudbreak.ALIGNMENT_SEPARATOR + "bar" + Cloudbreak.READ_SEPARATOR;
        alignments = reader.parsePairAlignmentLine(oeaLine);
        assertEquals(2, alignments.getRead1Alignments().size());
        assertEquals(0, alignments.getRead2Alignments().size());
    }

    @Test
    public void testGetChromosomeName() {
        SAMAlignmentReader reader = new SAMAlignmentReader();
        reader.setStripChromosomeNameAtWhitespace(true);
        SAMRecord record = (SAMRecord) reader.parseRecord("ERR024163.10000199/1\t322\t9 dna\t46892287\t255\t100M\t*\t0\t0\tCCATCTTTGCAACTCTATGTAAATCTAAAATTATTCCAGAGAATGACTTCACAAGGTGGCAGAATAGAATTTCTCCAGCTCCACACCCCCACACGGAAAT\tFFEDFD?EEGE@DBDEFEBFEEEB?BDECAEAFDFAAGFGGGEGGGGDGGFGGGGGGAGGFGGGGFFGEGGGEGGEGGGGEEAE?EFEFFGEGGGGGGGE\tRG:Z:0\tNM:i:0\tXT:A:R\tmd:Z:100");
        assertEquals("9", record.getChromosomeName());
        reader.setStripChromosomeNameAtWhitespace(false);
        record = (SAMRecord) reader.parseRecord("ERR024163.10000199/1\t322\t9 dna\t46892287\t255\t100M\t*\t0\t0\tCCATCTTTGCAACTCTATGTAAATCTAAAATTATTCCAGAGAATGACTTCACAAGGTGGCAGAATAGAATTTCTCCAGCTCCACACCCCCACACGGAAAT\tFFEDFD?EEGE@DBDEFEBFEEEB?BDECAEAFDFAAGFGGGEGGGGDGGFGGGGGGAGGFGGGGFFGEGGGEGGEGGGGEEAE?EFEFFGEGGGGGGGE\tRG:Z:0\tNM:i:0\tXT:A:R\tmd:Z:100");
        assertEquals("9 dna", record.getChromosomeName());
    }

}
