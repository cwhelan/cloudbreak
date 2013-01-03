package edu.ohsu.sonmezsysbio.cloudbreak;

import edu.ohsu.sonmezsysbio.cloudbreak.io.BaseAlignmentReader;
import edu.ohsu.sonmezsysbio.cloudbreak.io.SAMAlignmentReader;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 10/22/12
 * Time: 3:42 PM
 */
public class CloudbreakMapReduceBaseTest {

    @Test
    public void testParseSAMRecords() throws Exception {
        CloudbreakMapReduceBase mapper = new CloudbreakMapReduceBase();
        mapper.setAlignmentReader(new SAMAlignmentReader());

        String line = "2_41_211_0_1_0_0_2:0:0_3:0:0_116c37\t99\t2\t41\t70\t50M\t=\t212\t219\tACCCACACCCACACCCACACACACCACACCCACAGACCCCACCCACACCC" +
                "\t6332413100201//.0.0-/-/--/,.,,,.,.+-++++-+++,*,***\tLB:Z:JCVICHR2DIP\tMD:Z:34C3A11\tPG:Z:novoalign\tRG:Z:JCVICHR2DIP\tAM:i:70\tNM:i:2\tSM:i:70\tPQ:i:84\tUQ:i:35\tAS:i:35\tPU:Z:ILLUMINA\t" + "" +
                "SVP_READ\t2_41_211_0_1_0_0_2:0:0_3:0:0_116c37\t147\t2\t212\t70\t1S49M\t=\t41\t-219\tGACCCGCACCCTAACCCTAACCCTAACCCCTGACCCTAACCCCTAACCCT\t*,****+-+++---++,...,,,.//----0.0../11200013342336\tLB:Z:JCVICHR2DIP\tMD:Z:5A24A18\tPG:Z:novoalign\tRG:Z:JCVICHR2DIP\tAM:i:70\tNM:i:2\tSM:i:70\tPQ:i:84\tUQ:i:49\tAS:i:49\tPU:Z:ILLUMINA";

        ReadPairAlignments rpa = mapper.alignmentReader.parsePairAlignmentLine(line);
        SAMRecord r1 = (SAMRecord) rpa.getRead1Alignments().get(0);
        SAMRecord r2 = (SAMRecord) rpa.getRead2Alignments().get(0);

        assertTrue(r1.isForward());
        assertTrue(! r2.isForward());

    }


}
