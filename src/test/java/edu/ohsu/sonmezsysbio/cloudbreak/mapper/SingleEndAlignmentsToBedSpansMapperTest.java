package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.PairedAlignmentScorer;
import edu.ohsu.sonmezsysbio.cloudbreak.ProbabilisticPairedAlignmentScorer;
import edu.ohsu.sonmezsysbio.cloudbreak.file.FaidxFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.io.NovoalignAlignmentReader;
import edu.ohsu.sonmezsysbio.cloudbreak.io.SAMAlignmentReader;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GMMScorerResults;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GenotypingGMMScorer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static junit.framework.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 2/28/12
 * Time: 4:18 PM
 */
public class SingleEndAlignmentsToBedSpansMapperTest {

    SingleEndAlignmentsToBedSpansMapper mapper;

    @Before
    public void setup() {
        mapper = new SingleEndAlignmentsToBedSpansMapper();
        mapper.setAlignmentReader(new NovoalignAlignmentReader());
    }

    @Test
    public void testParseRegion() {
        mapper.parseRegion("1:54817520-54915143");
        assertEquals("1", mapper.getChromosome());
        assertEquals(54817520, mapper.getRegionStart());
        assertEquals(54915143, mapper.getRegionEnd());
    }
    private static class MockOutputCollector implements OutputCollector<Text, Text> {

        List<Text> keys = new ArrayList<Text>();
        List<Text> values = new ArrayList<Text>();

        public void collect(Text key, Text value) throws IOException {
            keys.add(key);
            values.add(value);
        }

        public void reset() {
            keys.clear();
            values.clear();
        }
    }

    @Test
    public void testEmitConcordantAlignmentIfFound() throws Exception {
        File testInput = new File(getClass().getResource("test_alignment.txt").getFile());
        String content = new Scanner(testInput).useDelimiter("\\Z").next();
        String key = content.substring(0,content.indexOf("\t"));
        String val = content.substring(content.indexOf("\t") + 1);

        MockOutputCollector mockOutputCollector = new MockOutputCollector();
        mapper.setAlignmentReader(new SAMAlignmentReader());
        mapper.setMaxInsertSize(25000);
        mapper.setTargetIsize(300);
        mapper.setTargetIsizeSD(30);
        mapper.setScorer(new ProbabilisticPairedAlignmentScorer());
        mapper.setChromosome("2");
        mapper.setRegionStart(27615761);
        mapper.setRegionEnd(27634000);

        mapper.map(new Text(key), new Text(val), mockOutputCollector, null);

    }

}
