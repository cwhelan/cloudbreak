package edu.ohsu.sonmezsysbio.cloudbreak.file;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/25/12
 * Time: 2:03 PM
 */
public class BigWigFileHelperTest {
    @Test
    public void testReadBigWigFile() throws Exception {
        BigWigFileHelper helper = new BigWigFileHelper();
        helper.open(getClass().getResource("wigVarStepExample.bw").getFile());
        //todo
        assertEquals(40.0,
                helper.getAverageValueForRegion(
                        "chr21", 9411295, 9411315),
                0.0001);

        assertEquals(41.1765,
                helper.getAverageValueForRegion(
                        "chr21", 9411296, 9411313),
                0.0001);

        assertEquals(20.0, helper.getMinValueForRegion("chr21", 9411296, 9411313), 0.0001);

    }


}
