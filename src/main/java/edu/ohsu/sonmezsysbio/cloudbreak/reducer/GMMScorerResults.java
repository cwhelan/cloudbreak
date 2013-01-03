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
    public double nodelOneComponentLikelihood;
    public double twoComponentLikelihood;
    public double lrHeterozygous;

    public int cleanCoverage;
    public double c1membership;
    public double c2membership;
    public double weightedC1membership;
    public double weightedC2membership;

    @Override
    public String toString() {
        return "GMMScorerResults{" +
                "w0=" + w0 +
                ", mu2=" + mu2 +
                ", nodelOneComponentLikelihood=" + nodelOneComponentLikelihood +
                ", twoComponentLikelihood=" + twoComponentLikelihood +
                ", lrHeterozygous=" + lrHeterozygous +
                ", cleanCoverage=" + cleanCoverage +
                ", c1membership=" + c1membership +
                ", c2membership=" + c2membership +
                ", weightedC1membership=" + weightedC1membership +
                ", weightedC2membership=" + weightedC2membership +
                '}';
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(w0);
        dataOutput.writeDouble(mu2);
        dataOutput.writeDouble(nodelOneComponentLikelihood);
        dataOutput.writeDouble(twoComponentLikelihood);
        dataOutput.writeDouble(lrHeterozygous);
        dataOutput.writeInt(cleanCoverage);
        dataOutput.writeDouble(c1membership);
        dataOutput.writeDouble(c2membership);
        dataOutput.writeDouble(weightedC1membership);
        dataOutput.writeDouble(weightedC2membership);
    }

    public void readFields(DataInput dataInput) throws IOException {
        w0 = dataInput.readDouble();
        mu2 = dataInput.readDouble();
        nodelOneComponentLikelihood = dataInput.readDouble();
        twoComponentLikelihood = dataInput.readDouble();
        lrHeterozygous = dataInput.readDouble();
        cleanCoverage = dataInput.readInt();
        c1membership = dataInput.readDouble();
        c2membership = dataInput.readDouble();
        weightedC1membership = dataInput.readDouble();
        weightedC2membership = dataInput.readDouble();
    }
}
