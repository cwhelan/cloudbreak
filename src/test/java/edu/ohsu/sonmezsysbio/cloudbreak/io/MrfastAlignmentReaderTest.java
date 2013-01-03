package edu.ohsu.sonmezsysbio.cloudbreak.io;

import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.MrfastAlignmentRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/8/12
 * Time: 4:39 PM
 */
public class MrfastAlignmentReaderTest {

    @Test
    public void testComputeAlignmentScores() throws Exception {
        MrfastAlignmentRecord record1 = new MrfastAlignmentRecord();
        record1.setMismatches(0);
        record1.setSequenceLength(50);

        List<AlignmentRecord> records = new ArrayList<AlignmentRecord>();
        records.add(record1);

        MrfastAlignmentReader reader = new MrfastAlignmentReader();
        assertEquals(50, reader.alignmentScore(record1), 0.00001);

        assertEquals(50, MrfastAlignmentReader.computeAlignmentNormalization(records), 0.000001);

    }

    @Test
    public void testParseChromosome() throws Exception {
        MrfastAlignmentReader reader = new MrfastAlignmentReader();
        assertEquals("2", reader.parseChromosome("2 dna:chromosome chromosome:NCBI36:2:1:242951149:1"));
    }
}
