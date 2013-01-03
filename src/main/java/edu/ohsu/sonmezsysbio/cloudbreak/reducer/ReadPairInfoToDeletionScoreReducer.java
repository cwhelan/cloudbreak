package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import edu.ohsu.sonmezsysbio.cloudbreak.ReadGroupInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.file.ReadGroupInfoFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocationWithQuality;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
import edu.ohsu.sonmezsysbio.svpipeline.io.GenomicLocation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/6/12
 * Time: 1:35 PM
 */
public class ReadPairInfoToDeletionScoreReducer extends MapReduceBase implements Reducer<GenomicLocationWithQuality, ReadPairInfo, GenomicLocation, DoubleWritable> {

    private static org.apache.log4j.Logger log = Logger.getLogger(ReadPairInfoToDeletionScoreReducer.class);

    //{ log.setLevel(Level.DEBUG); }

    ReadPairInfoScorer readPairInfoScorer = new IncrementalDelBeliefUpdateReadPairInfoScorer();

    public ReadPairInfoScorer getReadPairInfoScorer() {
        return readPairInfoScorer;
    }

    public void setReadPairInfoScorer(ReadPairInfoScorer readPairInfoScorer) {
        this.readPairInfoScorer = readPairInfoScorer;
    }

    private Map<Short,ReadGroupInfo> readGroupInfos;

    public Map<Short, ReadGroupInfo> getReadGroupInfos() {
        return readGroupInfos;
    }

    public void setReadGroupInfos(Map<Short, ReadGroupInfo> readGroupInfos) {
        this.readGroupInfos = readGroupInfos;
    }

    public void reduce(GenomicLocationWithQuality key, Iterator<ReadPairInfo> values, OutputCollector<GenomicLocation, DoubleWritable> output, Reporter reporter) throws IOException {
        log.debug("reducing for key: " + key);
        double lr = readPairInfoScorer.reduceReadPairInfos(values, readGroupInfos);
        log.debug("got score: " + lr);
        output.collect(new GenomicLocation(key.chromosome, key.pos), new DoubleWritable(lr));
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);

        String readGroupInfoFile = job.get("read.group.info.file");
        ReadGroupInfoFileHelper readGroupInfoFileHelper = new ReadGroupInfoFileHelper();
        try {
            readGroupInfos = readGroupInfoFileHelper.readReadGroupsById(readGroupInfoFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

