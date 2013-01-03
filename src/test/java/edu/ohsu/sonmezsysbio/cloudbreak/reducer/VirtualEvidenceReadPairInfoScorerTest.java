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
 * Date: 8/21/12
 * Time: 11:36 AM
 */
public class VirtualEvidenceReadPairInfoScorerTest {

    private VirtualEvidenceReadPairInfoScorer scorer;
    private Map<Short,ReadGroupInfo> readGroupInfos;

    @Before
    public void setup() throws Exception {

        readGroupInfos = new HashMap<Short, ReadGroupInfo>();
        ReadGroupInfo rg1 = new ReadGroupInfo();
        rg1.isize = 200;
        rg1.isizeSD = 30;
        rg1.matePair = false;
        readGroupInfos.put((short) 0, rg1);

        scorer = new VirtualEvidenceReadPairInfoScorer();
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
        assertTrue("score for only weak evidence should be negative", scorer.reduceReadPairInfos(readPairInfos.iterator(), readGroupInfos) < 0);
    }

    @Test
    public void testTwoStrongReadPairs() throws Exception {
        //ReadPairInfo readPairInfo1 = new ReadPairInfo(3000, -10.69, (short) 0);
        ReadPairInfo readPairInfo2 = new ReadPairInfo(3000, -0.01, (short) 0);
        //ReadPairInfo readPairInfo3 = new ReadPairInfo(3000, -8.32, (short) 0);
        ReadPairInfo readPairInfo4 = new ReadPairInfo(3000, -0.02, (short) 0);

        List<ReadPairInfo> readPairInfos = new ArrayList<ReadPairInfo>();
        //readPairInfos.add(readPairInfo1);
        readPairInfos.add(readPairInfo2);
        //readPairInfos.add(readPairInfo3);
        readPairInfos.add(readPairInfo4);

        // should get positive on this evidence
        assertEquals(-33.39, scorer.reduceReadPairInfos(readPairInfos.iterator(), readGroupInfos), 0.01);
    }

}
