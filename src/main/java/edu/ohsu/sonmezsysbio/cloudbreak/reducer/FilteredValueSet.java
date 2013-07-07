package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import java.util.HashMap;
import java.util.Map;

/**
* Created by IntelliJ IDEA.
* User: cwhelan
* Date: 7/5/13
* Time: 4:02 PM
*/
public class FilteredValueSet {
    public Integer maxIndex;
    Map<Integer,Double> values = new HashMap<Integer,Double>();

    public Double getVal(Integer idx) {
        return values.keySet().contains(idx) ? values.get(idx) : 0;
    }

    public void putVal(Integer idx, Double val) {
        values.put(idx, val);
    }
}
