package edu.ohsu.sonmezsysbio.cloudbreak.util;

import edu.ohsu.sonmezsysbio.cloudbreak.reducer.FilteredValueSet;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GMMResultsToVariantCallsReducer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 7/5/13
 * Time: 10:56 PM
 */
public class MedianFilterTest {
    @Test
    public void testSimpleMedianFilter() throws Exception {
        List<Text> input = new ArrayList<Text>();
        input.add(new Text("0\t2.1\t350\t4\t"));
        input.add(new Text("25\t2\t350\t4\t"));
        input.add(new Text("50\t1\t350\t4\t"));
        input.add(new Text("75\t2\t350\t4\t"));
        input.add(new Text("100\t2\t350\t4\t"));

        GMMResultsToVariantCallsReducer.GMMResultsIterator iterator = new GMMResultsToVariantCallsReducer.GMMResultsIterator(input.iterator());

        FilteredValueSet[] res = MedianFilter.medianFilterValues(iterator, 3, 1.5, 100l, 25);
        FilteredValueSet lrs = res[0];
        FilteredValueSet mus = res[1];

        assertEquals(2.1, lrs.getVal(0), 0.01);
        assertEquals(350.0, mus.getVal(0), 0.01);

        assertEquals(2.0, lrs.getVal(1), 0.01);
        assertEquals(350.0, mus.getVal(1), 0.01);

        assertEquals(1.0, lrs.getVal(2), 0.01);
        assertEquals(350.0, mus.getVal(2), 0.01);

        assertEquals(2.0, lrs.getVal(3), 0.01);
        assertEquals(350.0, mus.getVal(3), 0.01);
    }

    @Test
    public void testPeak() throws Exception {
        List<Text> input = new ArrayList<Text>();
        input.add(new Text("0\t0.5\t300\t4\t"));
        input.add(new Text("25\t0.3\t351\t4\t"));
        input.add(new Text("50\t0.6\t352\t4\t"));
        input.add(new Text("75\t1.5\t353\t4\t"));
        input.add(new Text("100\t2.0\t354\t4\t"));
        input.add(new Text("125\t1.3\t355\t4\t"));
        input.add(new Text("150\t0.4\t356\t4\t"));
        input.add(new Text("175\t1.1\t357\t4\t"));
        input.add(new Text("200\t0.5\t358\t4\t"));
        input.add(new Text("225\t0.5\t359\t4\t"));
        input.add(new Text("250\t0.5\t350\t4\t"));

        GMMResultsToVariantCallsReducer.GMMResultsIterator iterator = new GMMResultsToVariantCallsReducer.GMMResultsIterator(input.iterator());

        FilteredValueSet[] res = MedianFilter.medianFilterValues(iterator, 5, 1.0, 275l, 25);
        FilteredValueSet lrs = res[0];
        FilteredValueSet mus = res[1];

        assertEquals(0.0, lrs.getVal(0), 0.01);
        assertEquals(0.0, lrs.getVal(1), 0.01);
        assertEquals(0.0, lrs.getVal(2), 0.01);
        assertEquals(1.5, lrs.getVal(3), 0.01);
        assertEquals(2.0, lrs.getVal(4), 0.01);
        assertEquals(1.3, lrs.getVal(5), 0.01);
        assertEquals(0.4, lrs.getVal(6), 0.01);
        assertEquals(0.0, lrs.getVal(7), 0.01);
        assertEquals(0.0, lrs.getVal(8), 0.01);
        assertEquals(0.0, lrs.getVal(9), 0.01);
        assertEquals(0.0, lrs.getVal(10), 0.01);

        assertEquals(0.0, mus.getVal(0), 0.01);
        assertEquals(351.0, mus.getVal(1), 0.01);
        assertEquals(352.0, mus.getVal(2), 0.01);
        assertEquals(353.0, mus.getVal(3), 0.01);
        assertEquals(354.0, mus.getVal(4), 0.01);
        assertEquals(355.0, mus.getVal(5), 0.01);
        assertEquals(356.0, mus.getVal(6), 0.01);
        assertEquals(357.0, mus.getVal(7), 0.01);
        assertEquals(358.0, mus.getVal(8), 0.01);
        assertEquals(0.0, mus.getVal(9), 0.01);
        assertEquals(0.0, mus.getVal(10), 0.01);

    }

}
