package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 10/18/12
 * Time: 1:05 PM
 */
public class GMMScorerResults implements Writable {
    public double w0;
    public double mu2;
    public double lrHeterozygous;

    @Override
    public String toString() {
        return "GMMScorerResults{" +
                "w0=" + w0 +
                ", mu2=" + mu2 +
                ", lrHeterozygous=" + lrHeterozygous +
                '}';
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(w0);
        dataOutput.writeDouble(mu2);
        dataOutput.writeDouble(lrHeterozygous);
    }

    public void readFields(DataInput dataInput) throws IOException {
        w0 = dataInput.readDouble();
        mu2 = dataInput.readDouble();
        lrHeterozygous = dataInput.readDouble();
    }
}
