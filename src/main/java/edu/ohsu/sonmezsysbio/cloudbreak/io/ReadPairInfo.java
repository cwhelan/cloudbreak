package edu.ohsu.sonmezsysbio.cloudbreak.io;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/6/12
 * Time: 12:58 PM
 */
public class ReadPairInfo implements Writable {
    public int insertSize;
    public double pMappingCorrect;
    public Short readGroupId;

    public ReadPairInfo() {
    }

    public ReadPairInfo(int insertSize, double pMappingCorrect, Short readGroupId) {
        this.insertSize = insertSize;
        this.pMappingCorrect = pMappingCorrect;
        this.readGroupId = readGroupId;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(insertSize);
        out.writeDouble(pMappingCorrect);
        out.writeShort(readGroupId);
    }

    public void readFields(DataInput in) throws IOException {
        insertSize = in.readInt();
        pMappingCorrect = in.readDouble();
        readGroupId = in.readShort();
    }

    @Override
    public String toString() {
        return "ReadPairInfo{" +
                "insertSize=" + insertSize +
                ", pMappingCorrect=" + pMappingCorrect +
                ", readGroupId=" + readGroupId +
                '}';
    }
}
