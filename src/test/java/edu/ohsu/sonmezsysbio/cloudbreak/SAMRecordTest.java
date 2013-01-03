package edu.ohsu.sonmezsysbio.cloudbreak;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/23/11
 * Time: 10:50 AM
 */
public class SAMRecordTest {
    @Test
    public void testFlags() throws Exception {
        SAMRecord samRecord = new SAMRecord();
        samRecord.setFlag(403);
        assertTrue(samRecord.isMapped());
        assertTrue(samRecord.isMateMapped());
        assertTrue(samRecord.isPairMapped());

        assertTrue(samRecord.isReverseComplemented());

        samRecord.setFlag(355);
        assertTrue(! samRecord.isReverseComplemented());
    }

    @Test
    public void testPairPosteriorTag() throws Exception {
        SAMRecord samRecord = new SAMRecord();
        samRecord.addTag("PQ:i:340");
        assertEquals(340, samRecord.getPairPosterior());
    }

    @Test
    public void testOrientationFlags() throws Exception {
        SAMRecord r1 = new SAMRecord();
        r1.setFlag(99);

        SAMRecord r2 = new SAMRecord();
        r2.setFlag(147);

        assertTrue(r1.isForward());
        assertTrue(! r2.isForward());
    }

    @Test
    public void testGetSequenceLength() throws Exception {
        SAMRecord r = new SAMRecord();
        r.cigar = "100M";
        assertEquals(100, r.getSequenceLength());

        r.cigar = "31M1D69M";
        assertEquals(100, r.getSequenceLength());

        r.cigar = "70M1I29M";
        assertEquals(100, r.getSequenceLength());
    }

}
