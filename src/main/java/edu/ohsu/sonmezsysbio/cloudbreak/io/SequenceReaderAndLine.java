package edu.ohsu.sonmezsysbio.cloudbreak.io;

import edu.ohsu.sonmezsysbio.svpipeline.io.GenomicLocation;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

/**
* Created by IntelliJ IDEA.
* User: cwhelan
* Date: 4/10/12
* Time: 11:42 AM
*/
public class SequenceReaderAndLine implements ReaderAndLine {
    protected SequenceFile.Reader dataInput;
    protected GenomicLocation genomicLocation;
    protected Double nextValue;

    public SequenceFile.Reader getDataInput() {
        return dataInput;
    }

    public void setDataInput(SequenceFile.Reader dataInput) {
        this.dataInput = dataInput;
    }

    public GenomicLocation getGenomicLocation() {
        return genomicLocation;
    }

    public void setGenomicLocation(GenomicLocation genomicLocation) {
        this.genomicLocation = genomicLocation;
    }

    public Double getNextValue() {
        return nextValue;
    }

    public void setNextValue(Double nextValue) {
        this.nextValue = nextValue;
    }

    public SequenceReaderAndLine(SequenceFile.Reader dataInput, GenomicLocation genomicLocation, Double nextValue) {
        this.dataInput = dataInput;
        this.genomicLocation = genomicLocation;
        this.nextValue = nextValue;
    }

    public int compareTo(ReaderAndLine o) {
        return this.genomicLocation.compareTo(o.getGenomicLocation());
    }

    public void closeInput() throws IOException {
        dataInput.close();
    }
}
