package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.ProbabilisticPairedAlignmentScorer;
import edu.ohsu.sonmezsysbio.cloudbreak.file.FaidxFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.file.GFFFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocationWithQuality;
import edu.ohsu.sonmezsysbio.cloudbreak.io.NovoalignAlignmentReader;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.io.SAMAlignmentReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/6/12
 * Time: 4:58 PM
 */
public class SingleEndAlignmentsToReadPairInfoMapperTest {

    private SingleEndAlignmentsToReadPairInfoMapper mapper;

    @Before
    public void setup() {
        mapper = new SingleEndAlignmentsToReadPairInfoMapper();
        mapper.setScorer(new ProbabilisticPairedAlignmentScorer());
        mapper.setAlignmentReader(new NovoalignAlignmentReader());
        mapper.setFaix(new FaidxFileHelper("foo") {
            @Override
            public Short getKeyForChromName(String name) throws IOException {
                assertEquals("10", name);
                return (short) 9;
            }
        });

    }

    @Test
    public void testMapPairedEnd() throws Exception {

        String key = "@ERR000545.10000001 EAS139_44:1:93:532:453";
        String value = "@ERR000545.10000001 EAS139_44:1:93:532:453/1\tS\tCAAAAACCACTTGTACTCCAAAAGCTATTGAAGTTTAAGTTAAAATAAAAA\t<??>>?;<>=@?=?>8@<<9=98=:@>>>=:>>:6?7>9:?<46:;9;.:9\tR\t114\t0.00000\t>10\t43049466\tR\t.\t.\t.\t10A>C 22C>A 46-A\tSVP_READ\t@ERR000545.10000001 EAS139_44:1:93:532:453/2\tS\tTTATTGCACTTACCATGACTGTCTTCTGAAATGCATCTCAACCCTTGAATA\t;<8>>:$>=?@?>>:>=>:9=8<>1;8:<=>>9=:=7><>=;=;=>=72:>\tU\t34\t145.2313\t>10\t43039500\tF\t.\t.\t.\t3G>A";

        mapper.setReadGroupId((short) 3);

        MockOutputCollector collector = new MockOutputCollector();
        Reporter reporter = Mockito.mock(Reporter.class);

        mapper.map(new Text(key), new Text(value), collector, reporter);

        int idx = 0;
        for (int i = 43039500; i <= 43049500; i = i + Cloudbreak.DEFAULT_RESOLUTION) {
            GenomicLocationWithQuality genomicLocationWithQuality = new GenomicLocationWithQuality();
            genomicLocationWithQuality.chromosome = 9;
            genomicLocationWithQuality.pos = i;
            genomicLocationWithQuality.pMappingCorrect = -9.210340371976185;

            assertTrue(collector.keys.contains(genomicLocationWithQuality));

            assertEquals(10017, collector.values.get(collector.keys.indexOf(genomicLocationWithQuality)).insertSize);
            assertEquals(-9.2103, collector.values.get(collector.keys.indexOf(genomicLocationWithQuality)).pMappingCorrect, .0001);
            assertEquals((short) 3, (short) collector.values.get(collector.keys.indexOf(genomicLocationWithQuality)).readGroupId);
            idx++;
        }

        assertEquals(idx, collector.keys.size());

    }

    @Test
    public void testMapPairedEndWithSegmentalDuplications() throws Exception {

        String key = "@ERR000545.10000001 EAS139_44:1:93:532:453";
        String value = "@ERR000545.10000001 EAS139_44:1:93:532:453/1\tS\tCAAAAACCACTTGTACTCCAAAAGCTATTGAAGTTTAAGTTAAAATAAAAA\t<??>>?;<>=@?=?>8@<<9=98=:@>>>=:>>:6?7>9:?<46:;9;.:9\tR\t114\t0.00000\t>10\t43049466\tR\t.\t.\t.\t10A>C 22C>A 46-A\tSVP_READ\t@ERR000545.10000001 EAS139_44:1:93:532:453/2\tS\tTTATTGCACTTACCATGACTGTCTTCTGAAATGCATCTCAACCCTTGAATA\t;<8>>:$>=?@?>>:>=>:9=8<>1;8:<=>>9=:=7><>=;=;=>=72:>\tU\t34\t145.2313\t>10\t43039500\tF\t.\t.\t.\t3G>A";

        mapper.setExclusionRegions(new GFFFileHelper() {
            @Override
            public boolean doesLocationOverlap(String chrom, int start, int end) throws Exception {
                if ("10".equals(chrom) &&
                        ((start > 43039000 && start < 43040000) && (end > 43039000 && end < 43040000)) ||
                        ((start > 43049000 && start < 43050000) && (end > 43049000 && end < 43050000))) {
                    return true;
                }
                return false;
            }
        });

        MockOutputCollector collector = new MockOutputCollector();
        Reporter reporter = Mockito.mock(Reporter.class);

        mapper.map(new Text(key), new Text(value), collector, reporter);

        assertEquals(0, collector.keys.size());

        key = "@ERR000545.10000001 EAS139_44:1:93:532:453";
        value = "@ERR000545.10000001 EAS139_44:1:93:532:453/1\tS\tCAAAAACCACTTGTACTCCAAAAGCTATTGAAGTTTAAGTTAAAATAAAAA\t<??>>?;<>=@?=?>8@<<9=98=:@>>>=:>>:6?7>9:?<46:;9;.:9\tR\t114\t0.00000\t>10\t49466\tR\t.\t.\t.\t10A>C 22C>A 46-A\tSVP_READ\t@ERR000545.10000001 EAS139_44:1:93:532:453/2\tS\tTTATTGCACTTACCATGACTGTCTTCTGAAATGCATCTCAACCCTTGAATA\t;<8>>:$>=?@?>>:>=>:9=8<>1;8:<=>>9=:=7><>=;=;=>=72:>\tU\t34\t145.2313\t>10\t39500\tF\t.\t.\t.\t3G>A";
        collector = new MockOutputCollector();
        mapper.map(new Text(key), new Text(value), collector, reporter);
        assertEquals(101, collector.keys.size());
    }

    private static class MockOutputCollector implements OutputCollector<GenomicLocationWithQuality, ReadPairInfo> {

        List<GenomicLocationWithQuality> keys = new ArrayList<GenomicLocationWithQuality>();
        List<ReadPairInfo> values = new ArrayList<ReadPairInfo>();

        public void collect(GenomicLocationWithQuality key, ReadPairInfo value) throws IOException {
            keys.add(key);
            values.add(value);
        }

        public void reset() {
            keys.clear();
            values.clear();
        }
    }

    @Test
    public void testMap_real1() throws Exception {
        String key = "@2_132385096_132385222_0_1_0_0_1:0:0_1:0:0_1bfnjd/";
        String val = "@2_132385096_132385222_0_1_0_0_1:0:0_1:0:0_1bfnjd//1\tS\tTAAAAAGCCGCGGCGACTAAAAGCCGCTGAGAGGGGGCAAAAAGCAGCGG\t66554410000////1.0000/----,/,.,.,,++++-----+*-****\tU\t25\t139.09267\t>2\t132512583\tF\t.\t.\t.\t18A>T\tSVP_READ\t@2_132385096_132385222_0_1_0_0_1:0:0_1:0:0_1bfnjd//2\tS\tCCCCTGCCCCGCCGCGGCTTTTTGCGGCTTTCCGCCCCGGCCGCCGCGGA\t33324110000////...000//---,,...,,,++++++++++*****,\tU\t23\t135.68983\t>2\t132512814\tR\t.\t.\t.\t1G>T";
        String va2 = "@2_132385096_132385222_0_1_0_0_1:0:0_1:0:0_1bf17d//1\tS\tTAAAAAGCCGCGGCGACTAAAAGCCGCTGAGAGGGGGCAAAAAGCAGCGG\t66554410000////1.0000/----,/,.,.,,++++-----+*-****\tU\t25\t139.09267\t>2\t132512583\tF\t.\t.\t.\t18A>T\tSVP_READ\t@2_132385096_132385222_0_1_0_0_1:0:0_1:0:0_1bf17d//2\tS\tCCCCTGCCCCGCCGCGGCTTTTTGCGGCTTTCCGCCCCGGCCGCCGCGGA\t33324110000////...000//---,,...,,,++++++++++*****,\t23\t135.68983\t>2\t132512814\tR\t.\t.\t.\t1G>T";

        mapper.setFaix(new FaidxFileHelper("foo") {
            @Override
            public Short getKeyForChromName(String name) throws IOException {
                assertEquals("2", name);
                return (short) 0;
            }
        });

        MockOutputCollector mockOutputCollector = new MockOutputCollector();
        mapper.map(new Text(key), new Text(val), mockOutputCollector, null);
        mapper.setChromosomeFilter("2");
        mapper.setStartFilter(132512600l);
        mapper.setEndFilter(132512800l);

        GenomicLocationWithQuality gl132512700 = new GenomicLocationWithQuality();
        gl132512700.chromosome = 0;
        gl132512700.pos = 132512700;
        gl132512700.pMappingCorrect = -3.9301895071730983E-14;

        assertTrue(mockOutputCollector.keys.contains(gl132512700));
        assertEquals(281, mockOutputCollector.values.get(0).insertSize);
    }

    @Test
    public void testGetInputPath() throws Exception {
        assertEquals("/user/whelanch/cloudbreak/jcvi_chr2_lc/se_alignments_t180/part-00000",
                SingleEndAlignmentsMapper.getInputPath("hdfs://bigbird51.csee.ogi.edu:50030/user/whelanch/cloudbreak/jcvi_chr2_lc/se_alignments_t180/part-00000"));
    }

    @Test
    public void testEmitConcordantAlignmentIfFound() throws Exception {
        File testInput = new File(getClass().getResource("test_alignment.txt").getFile());
        String content = new Scanner(testInput).useDelimiter("\\Z").next();
        String key = content.substring(0,content.indexOf("\t"));
        String val = content.substring(content.indexOf("\t") + 1);

        MockOutputCollector mockOutputCollector = new MockOutputCollector();
        mapper.setAlignmentReader(new SAMAlignmentReader());
        mapper.setFaix(new FaidxFileHelper("foo") {
            @Override
            public Short getKeyForChromName(String name) throws IOException {
                assertEquals("2", name);
                return (short) 9;
            }
        });
        mapper.setMaxInsertSize(25000);
        mapper.setTargetIsize(300);
        mapper.setTargetIsizeSD(30);
        mapper.map(new Text(key), new Text(val), mockOutputCollector, null);

    }

    @Test
    public void testMultipleConcordantMappings() throws Exception {
        File testInput = new File(getClass().getResource("4f9f9_fix.txt").getFile());
        String content = new Scanner(testInput).useDelimiter("\\Z").next();
        String key = content.substring(0,content.indexOf("\t"));
        String val = content.substring(content.indexOf("\t") + 1);

        MockOutputCollector mockOutputCollector = new MockOutputCollector();
        mapper.setAlignmentReader(new SAMAlignmentReader());
        mapper.setFaix(new FaidxFileHelper("foo") {
            @Override
            public Short getKeyForChromName(String name) throws IOException {
                assertEquals("2", name);
                return (short) 9;
            }
        });
        mapper.setMaxInsertSize(2500);
        mapper.setTargetIsize(300);
        mapper.setTargetIsizeSD(30);
        mapper.map(new Text(key), new Text(val), mockOutputCollector, null);

        assertEquals(9, mockOutputCollector.keys.size());
        assertEquals(new HashSet(mockOutputCollector.keys).size(), mockOutputCollector.keys.size());
    }

    // todo: add assertions
    @Test
    public void testMultipleConcordantMappings2() throws Exception {
        File testInput = new File(getClass().getResource("e7421.txt").getFile());
        String content = new Scanner(testInput).useDelimiter("\\Z").next();
        String key = content.substring(0,content.indexOf("\t"));
        String val = content.substring(content.indexOf("\t") + 1);

        MockOutputCollector mockOutputCollector = new MockOutputCollector();
        mapper.setAlignmentReader(new SAMAlignmentReader());
        mapper.setFaix(new FaidxFileHelper("foo") {
            @Override
            public Short getKeyForChromName(String name) throws IOException {
                assertEquals("2", name);
                return (short) 9;
            }
        });
        mapper.setMaxInsertSize(2500);
        mapper.setTargetIsize(300);
        mapper.setTargetIsizeSD(30);
        mapper.map(new Text(key), new Text(val), mockOutputCollector, null);

    }

    // todo:add assertions
    @Test
    public void testAluPair() throws Exception {
        File testInput = new File(getClass().getResource("9fefc3.txt").getFile());
        String content = new Scanner(testInput).useDelimiter("\\Z").next();
        String key = content.substring(0,content.indexOf("\t"));
        String val = content.substring(content.indexOf("\t") + 1);

        MockOutputCollector mockOutputCollector = new MockOutputCollector();
        mapper.setAlignmentReader(new SAMAlignmentReader());
        mapper.setFaix(new FaidxFileHelper("foo") {
            @Override
            public Short getKeyForChromName(String name) throws IOException {
                assertEquals("2", name);
                return (short) 9;
            }
        });
        mapper.setMaxInsertSize(2500);
        mapper.setTargetIsize(300);
        mapper.setTargetIsizeSD(30);
        mapper.map(new Text(key), new Text(val), mockOutputCollector, null);

    }

}
