package edu.ohsu.sonmezsysbio.cloudbreak.io;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/30/12
 * Time: 2:14 PM
 */

/**
 * Compares GenomicLocation. For MapReduce
 * secondary sorting.
 */
public class GenomicLocationSortComparator implements RawComparator<GenomicLocation> {
    private final GenomicLocation key1;
    private final GenomicLocation key2;
    private final DataInputBuffer buffer;

    public GenomicLocationSortComparator() {
        key1 = new GenomicLocation();
        key2 = new GenomicLocation();
        buffer = new DataInputBuffer();
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            buffer.reset(b1, s1, l1);                   // parse key1
            key1.readFields(buffer);

            buffer.reset(b2, s2, l2);                   // parse key2
            key2.readFields(buffer);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return compare(key1, key2);                   // compare them
    }

    public int compare(GenomicLocation o1, GenomicLocation o2) {
        if (o1.chromosome < o2.chromosome) return  -1;
        if (o1.chromosome > o2.chromosome) return  1;
        if (o1.pos < o2.pos) return -1;
        if (o1.pos > o2.pos) return 1;
        return 0;
    }

}