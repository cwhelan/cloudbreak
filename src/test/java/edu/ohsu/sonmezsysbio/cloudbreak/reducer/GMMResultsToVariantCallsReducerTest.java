package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/12/13
 * Time: 3:41 PM
 */
public class GMMResultsToVariantCallsReducerTest {
    @Test
    public void testMuHasChangedTooMuch() throws Exception {
        double[] muVals = new double[4];
        muVals[0] = 12094.5;
        muVals[1] = 12094.5;
        muVals[2] = 8760.692;
        muVals[3] = 8772.477;

        assertTrue(GMMResultsToVariantCallsReducer.muHasChangedTooMuch(15, 2, muVals[2], muVals[(2 - 2)]));
    }


    @Test
    public void testSimpleDeletionCall() throws Exception {
        FilteredValueSet lrs = new FilteredValueSet();
        lrs.putVal(3, 2.5);
        lrs.putVal(4, 3.0);
        lrs.putVal(5, 2.0);
        lrs.maxIndex = 10;

        FilteredValueSet mus = new FilteredValueSet();
        mus.putVal(1, 350.0);
        mus.putVal(2, 350.0);
        mus.putVal(3, 350.0);
        mus.putVal(4, 350.0);
        mus.putVal(5, 350.0);
        mus.putVal(6, 350.0);
        mus.putVal(7, 350.0);
        lrs.maxIndex = 10;

        FilteredValueSet w0s = new FilteredValueSet();
        w0s.putVal(1,0.0);
        w0s.putVal(2,0.0);
        w0s.putVal(3,0.0);
        w0s.putVal(4,0.0);
        w0s.putVal(5,0.0);
        w0s.putVal(6,0.0);
        w0s.putVal(7,0.0);

        GMMResultsToVariantCallsReducer reducer = new GMMResultsToVariantCallsReducer();

        final List<String> regions = new ArrayList();
        reducer.writePositiveRegions(lrs, new OutputCollector<Text, Text>() {
            @Override
            public void collect(Text text, Text text2) throws IOException {
                regions.add(text2.toString());
            }
        }, "2", 25, 0, mus, w0s, 300, 30, Cloudbreak.VARIANT_TYPE_DELETION, 1.0, 10);

        assertEquals(1, regions.size());
        assertEquals("75\t149\t0\t3.0\tDEL\t350.0\t350.0\t350.0\t0.0\t0.0\t0.0", regions.get(0));
    }

    // tood: migrate and enable any of these tests that are still applicable after factoring out wigfilehelper
    /**
     *     @Test
    public void testExportPositiveRegionsFromWig() throws Exception {

    FaidxFileHelper faidx = new FaidxFileHelper("foo") {
    @Override
    public Long getLengthForChromName(String name) throws IOException {
    if ("chr1".equals(name)) return 4500l;
    if ("chr2".equals(name)) return 5000l;
    return null;
    }
    };

    // test chromosome transition
    String outputPrefix = "test";
    String wigFile = "variableStep chrom=chr1 span=1000\n" +
    "1000\t2.0\n" +
    "2000\t4.0\n" +
    "3000\t5.0\n" +
    "variableStep chrom=chr2 span=1000\n" +
    "1000\t7.0\n" +
    "2000\t6.0\n" +
    "3000\t3.0\n";
    BufferedReader wigFileReader = new BufferedReader(new StringReader(wigFile));

    StringWriter stringWriter = new StringWriter();
    BufferedWriter bedFileWriter = new BufferedWriter(stringWriter);
    double threshold = 4.5;

    WigFileHelper.exportRegionsOverThresholdFromWig(outputPrefix, wigFileReader, bedFileWriter, threshold, faidx, 1);
    bedFileWriter.close();

    String expectedOutput =
    "track name = \"test peaks over " + threshold + "\"\n" +
    "chr1\t3000\t3999\t1\t5.0\tNA\n" +
    "chr2\t1000\t2999\t2\t7.0\tNA\n";
    assertEquals(expectedOutput, stringWriter.getBuffer().toString());
    }

     @Test
     public void testExportPositiveRegionsFromWigWithExtraFile() throws Exception {

     FaidxFileHelper faidx = new FaidxFileHelper("foo") {
     @Override
     public Long getLengthForChromName(String name) throws IOException {
     if ("chr1".equals(name)) return 4500l;
     if ("chr2".equals(name)) return 5000l;
     return null;
     }
     };

     // test chromosome transition
     String outputPrefix = "test";
     String wigFile = "variableStep chrom=chr1 span=1000\n" +
     "1000\t2.0\n" +
     "2000\t4.0\n" +
     "3000\t5.0\n" +
     "variableStep chrom=chr2 span=1000\n" +
     "1000\t7.0\n" +
     "2000\t6.0\n" +
     "3000\t3.0\n";
     BufferedReader wigFileReader = new BufferedReader(new StringReader(wigFile));

     Map<String, BufferedReader> extraWigFileReaders = new HashMap<String, BufferedReader>();

     String extraWigFile = "variableStep chrom=chr1 span=1000\n" +
     "1000\t1.0\n" +
     "2000\t2.0\n" +
     "3000\t3.0\n" +
     "variableStep chrom=chr2 span=1000\n" +
     "1000\t5.0\n" +
     "2000\t3.0\n" +
     "3000\t2.0\n";
     BufferedReader extraWigFileReader = new BufferedReader(new StringReader(extraWigFile));
     extraWigFileReaders.put("foo", extraWigFileReader);

     String extraWigFile2 = "variableStep chrom=chr1 span=1000\n" +
     "1000\t1.0\n" +
     "2000\t1.0\n" +
     "3000\t1.0\n" +
     "variableStep chrom=chr2 span=1000\n" +
     "1000\t2.0\n" +
     "2000\t2.0\n" +
     "3000\t2.0\n";
     BufferedReader extraWigFileReader2 = new BufferedReader(new StringReader(extraWigFile2));
     extraWigFileReaders.put("bar", extraWigFileReader2);

     List<String> extraWigFiles = new ArrayList<String>();
     extraWigFiles.add("bar");
     extraWigFiles.add("foo");

     StringWriter stringWriter = new StringWriter();
     BufferedWriter bedFileWriter = new BufferedWriter(stringWriter);
     double threshold = 4.5;

     WigFileHelper.exportRegionsOverThresholdFromWig(outputPrefix, wigFileReader, bedFileWriter, threshold, faidx, 1,
     extraWigFiles, extraWigFileReaders);
     bedFileWriter.close();

     String expectedOutput =
     "track name = \"test peaks over " + threshold + "\"\n" +
     "chr1\t3000\t3999\t1\t5.0\tNA\t1.0\t1.0\t1.0\t3.0\t3.0\t3.0\n" +
     "chr2\t1000\t2999\t2\t7.0\tNA\t2.0\t2.0\t2.0\t4.0\t3.0\t5.0\n";
     assertEquals(expectedOutput, stringWriter.getBuffer().toString());
     }

     @Test
     public void testExportPositiveRegionsFromWigWithMedianFilter() throws Exception {
     FaidxFileHelper faidx = new FaidxFileHelper("foo") {
     @Override
     public Long getLengthForChromName(String name) throws IOException {
     if ("chr1".equals(name)) return 6500l;
     return null;
     }
     };
     String outputPrefix = "test";
     String wigFile = "variableStep chrom=chr1 span=1000\n" +
     "1000\t2.0\n" +
     "2000\t5.0\n" +
     "3000\t5.0\n" +
     "4000\t3.0\n" +
     "5000\t6.0\n" +
     "6000\t3.0\n";
     BufferedReader wigFileReader = new BufferedReader(new StringReader(wigFile));

     StringWriter stringWriter = new StringWriter();
     BufferedWriter bedFileWriter = new BufferedWriter(stringWriter);
     double threshold = 4.5;

     WigFileHelper.exportRegionsOverThresholdFromWig(outputPrefix, wigFileReader, bedFileWriter, threshold, faidx, 3);
     bedFileWriter.close();

     String expectedOutput =
     "track name = \"test peaks over " + threshold + "\"\n" +
     "chr1\t2000\t4999\t1\t5.0\tNA\n";
     assertEquals(expectedOutput, stringWriter.getBuffer().toString());

     }
     */
}
