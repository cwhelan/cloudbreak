package edu.ohsu.sonmezsysbio.cloudbreak;

import edu.ohsu.sonmezsysbio.cloudbreak.io.AlignmentReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/23/12
 * Time: 10:05 PM
 */

/**
 * This is a base class extended by mappers and reducers that provides access to cross-command configuration information.
 */
public class CloudbreakMapReduceBase extends MapReduceBase {

    protected int resolution = Cloudbreak.DEFAULT_RESOLUTION;
    protected AlignmentReader alignmentReader;
    private String alignerName;

    public int getResolution() {
        return resolution;
    }

    public void setResolution(int resolution) {
        this.resolution = resolution;
    }

    public String getAlignerName() {
        return alignerName;
    }

    public void setAlignerName(String alignerName) {
        this.alignerName = alignerName;
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        if (job.get("cloudbreak.resolution") != null) {
            resolution = Integer.parseInt(job.get("cloudbreak.resolution"));
        }
        if (job.get("cloudbreak.aligner") != null) {
            alignerName = job.get("cloudbreak.aligner");
            alignmentReader = AlignmentReader.AlignmentReaderFactory.getInstance(alignerName);
            alignmentReader.setLegacyAlignments(Boolean.parseBoolean(job.get("legacy.alignments")));
        }
    }

    public AlignmentReader getAlignmentReader() {
        return alignmentReader;
    }

    public void setAlignmentReader(AlignmentReader alignmentReader) {
        this.alignmentReader = alignmentReader;
    }

}
