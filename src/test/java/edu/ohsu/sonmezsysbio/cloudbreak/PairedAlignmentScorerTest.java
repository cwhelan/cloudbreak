package edu.ohsu.sonmezsysbio.cloudbreak;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 10/22/12
 * Time: 3:27 PM
 */
public class PairedAlignmentScorerTest {
    @Test
    public void testValidateMappingOrientations() throws Exception {
        SAMRecord r1 = new SAMRecord();
        r1.flag = 99;
        r1.referenceName = "2";
        r1.position = 41;
        r1.sequence = "..................................................";

        SAMRecord r2 = new SAMRecord();
        r2.flag = 147;
        r2.referenceName = "2";
        r2.position = 212;
        r2.sequence = "..................................................";

        PairedAlignmentScorer scorer = new PairedAlignmentScorer() {
            @Override
            public double computeDeletionScore(int insertSize, Double targetIsize, Double targetIsizeSD, Double pMappingCorrect) {
                return 0;
            }
        };
        assertTrue(scorer.validateMappingOrientations(r1, r2, false));
    }
}
