package edu.ohsu.sonmezsysbio.cloudbreak.file;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/19/12
 * Time: 1:23 PM
 */
public class GFFFileHelperTest {
    @Test
    public void testDoesLocationOverlap() throws Exception {
        GFFFileHelper gffFileHelper = new GFFFileHelper(getClass().getResource("testGFFFile.gff").getFile());
        assertTrue(gffFileHelper.doesLocationOverlap("1", 145311600, 1453116001));
        assertFalse(gffFileHelper.doesLocationOverlap("1", 1500, 1501));
        assertFalse(gffFileHelper.doesLocationOverlap("3", 145311600, 1453116001));
        assertTrue(gffFileHelper.doesLocationOverlap("4", 103817701, 103827701));
    }
}
