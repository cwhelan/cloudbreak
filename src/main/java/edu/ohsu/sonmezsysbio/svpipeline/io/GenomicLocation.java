package edu.ohsu.sonmezsysbio.svpipeline.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/6/12
 * Time: 1:05 PM
 *
 * todo: this class still in svpipeline for now to support legacy data
 */
public class GenomicLocation implements WritableComparable<GenomicLocation> {
    public short chromosome;
    public int pos;

    public GenomicLocation() {
    }

    public GenomicLocation(short chromosome, int pos) {
        this.chromosome = chromosome;
        this.pos = pos;
    }

    public void write(DataOutput out) throws IOException {
        out.writeShort(chromosome);
        out.writeInt(pos);
    }

    public void readFields(DataInput in) throws IOException {
        chromosome = in.readShort();
        pos = in.readInt();
    }

    public int compareTo(GenomicLocation o) {
        if (chromosome < o.chromosome) {
            return -1;
        } else if (chromosome > o.chromosome) {
            return 1;
        } else {
            if (pos < o.pos) return -1;
            if (pos > o.pos) return 1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GenomicLocation that = (GenomicLocation) o;

        if (chromosome != that.chromosome) return false;
        if (pos != that.pos) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) chromosome;
        result = 31 * result + pos;
        return result;
    }

    @Override
    public String toString() {
        return "GenomicLocation{" +
                "chromosome=" + chromosome +
                ", pos=" + pos +
                '}';
    }
}
