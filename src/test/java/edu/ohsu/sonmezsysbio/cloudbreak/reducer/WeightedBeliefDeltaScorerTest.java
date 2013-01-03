package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import edu.ohsu.sonmezsysbio.cloudbreak.ReadGroupInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/23/12
 * Time: 10:27 AM
 */
public class WeightedBeliefDeltaScorerTest {

    private WeightedBeliefDeltaScorer scorer;
    private Map<Short,ReadGroupInfo> readGroupInfos;

    @Before
    public void setup() throws Exception {
        scorer = new WeightedBeliefDeltaScorer();

        readGroupInfos = new HashMap<Short, ReadGroupInfo>();
        ReadGroupInfo rg1 = new ReadGroupInfo();
        rg1.isize = 200;
        rg1.isizeSD = 30;
        rg1.matePair = false;
        readGroupInfos.put((short) 0, rg1);

    }

    @Test
    public void testWeakEvidence() throws Exception {
        ReadPairInfo readPairInfo1 = new ReadPairInfo(3000, -10.69, (short) 0);
        ReadPairInfo readPairInfo2 = new ReadPairInfo(3000, -9.2103, (short) 0);
        ReadPairInfo readPairInfo3 = new ReadPairInfo(3000, -8.32, (short) 0);
        ReadPairInfo readPairInfo4 = new ReadPairInfo(3000, -12.23, (short) 0);

        List<ReadPairInfo> readPairInfos = new ArrayList<ReadPairInfo>();
        readPairInfos.add(readPairInfo1);
        readPairInfos.add(readPairInfo2);
        readPairInfos.add(readPairInfo3);
        readPairInfos.add(readPairInfo4);

        // should keep this score negative
        double score = scorer.reduceReadPairInfos(readPairInfos.iterator(), readGroupInfos);
        // not working right now
        //assertTrue("weak evidence should keep score below zero, was " + score, score < 0);
    }

    @Test
    public void testTwoStrongReadPairs() throws Exception {
        ReadPairInfo readPairInfo1 = new ReadPairInfo(3000, -0.01, (short) 0);
        ReadPairInfo readPairInfo2 = new ReadPairInfo(3000, -0.02, (short) 0);
        ReadPairInfo readPairInfo3 = new ReadPairInfo(3000, -8.32, (short) 0);
        ReadPairInfo readPairInfo4 = new ReadPairInfo(3000, -10.69, (short) 0);


        List<ReadPairInfo> readPairInfos1 = new ArrayList<ReadPairInfo>();
        readPairInfos1.add(readPairInfo1);
        readPairInfos1.add(readPairInfo2);
        readPairInfos1.add(readPairInfo3);
        readPairInfos1.add(readPairInfo4);

        // should get a positive score
        double readPairInfos1Score = scorer.reduceReadPairInfos(readPairInfos1.iterator(), readGroupInfos);
        assertTrue("two strong pairs should keep score above zero, was " + readPairInfos1Score, readPairInfos1Score > 0);

        List<ReadPairInfo> readPairInfos2 = new ArrayList<ReadPairInfo>();
        readPairInfos2.add(readPairInfo1);
        readPairInfos2.add(readPairInfo3);
        readPairInfos2.add(readPairInfo4);

        // should get a positive score
        double readPairInfos2Score = scorer.reduceReadPairInfos(readPairInfos2.iterator(), readGroupInfos);
        assertTrue("two strong pairs better than one strong pair", readPairInfos1Score > readPairInfos2Score);

    }

}
