package edu.ohsu.sonmezsysbio.cloudbreak.io;

import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GMMScorerResults;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 10/18/12
 * Time: 2:15 PM
 */

/**
 * Used by the export command, this class holds a sequence file reader to a sequence file with keys of
 * type GenomicLocation and values of type GMMScorerResults. Used to do an efficient merge of multiple
 * reduce output files in HDFS.
 */
public class GMMResultsReaderAndLine implements Comparable<GMMResultsReaderAndLine> {
    private SequenceFile.Reader reader;
    private GenomicLocation gl;
    private GMMScorerResults results;

    public GMMResultsReaderAndLine(SequenceFile.Reader reader, GenomicLocation gl, GMMScorerResults results) {
        this.reader = reader;
        this.gl = gl;
        this.results = results;
    }

    public GenomicLocation getGenomicLocation() {
        return gl;
    }

    public void setGenomicLocation(GenomicLocation genomicLocation) {
        this.gl = genomicLocation;
    }

    public GMMScorerResults getNextValue() {
        return results;
    }

    public void setNextValue(GMMScorerResults nextValue) {
        this.results = nextValue;
    }

    public SequenceFile.Reader getReader() {
        return reader;
    }

    public void closeInput() throws IOException {
        reader.close();
    }

    public int compareTo(GMMResultsReaderAndLine o) {
        return this.gl.compareTo(o.getGenomicLocation());
    }
}
