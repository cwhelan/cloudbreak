package edu.ohsu.sonmezsysbio.cloudbreak.io;

import org.junit.Test;

import static junit.framework.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/7/12
 * Time: 12:15 PM
 */
public class GenomicLocationTest {
    @Test
    public void testCompareTo() throws Exception {
        GenomicLocation g1 = new GenomicLocation((short) 3, 14535);
        GenomicLocation g2 = new GenomicLocation((short) 3, 24543534);

        assertTrue(g1.compareTo(g2) == -1);


    }
}
