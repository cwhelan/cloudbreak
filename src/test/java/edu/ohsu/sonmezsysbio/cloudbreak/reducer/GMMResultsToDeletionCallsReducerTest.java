package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/12/13
 * Time: 3:41 PM
 */
public class GMMResultsToDeletionCallsReducerTest {
    @Test
    public void testMuHasChangedTooMuch() throws Exception {
        double[] muVals = new double[4];
        muVals[0] = 12094.5;
        muVals[1] = 12094.5;
        muVals[2] = 8760.692;
        muVals[3] = 8772.477;

        assertTrue(GMMResultsToDeletionCallsReducer.muHasChangedTooMuch(muVals, 15, 3));
    }
}
