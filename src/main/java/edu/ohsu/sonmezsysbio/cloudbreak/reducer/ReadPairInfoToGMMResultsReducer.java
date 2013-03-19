package edu.ohsu.sonmezsysbio.cloudbreak.reducer;

import edu.ohsu.sonmezsysbio.cloudbreak.ReadGroupInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.file.ReadGroupInfoFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocation;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocationWithQuality;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
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
public class ReadPairInfoToGMMResultsReducer extends MapReduceBase implements Reducer<GenomicLocationWithQuality, ReadPairInfo, GenomicLocation, GMMScorerResults> {

    private static Logger log = Logger.getLogger(ReadPairInfoToGMMResultsReducer.class);

    { log.setLevel(Level.DEBUG); }

    GenotypingGMMScorer readPairInfoScorer = new GenotypingGMMScorer();

    public GenotypingGMMScorer getReadPairInfoScorer() {
        return readPairInfoScorer;
    }

    public void GenotypingGMMScorer(GenotypingGMMScorer readPairInfoScorer) {
        this.readPairInfoScorer = readPairInfoScorer;
    }

    private Map<Short,ReadGroupInfo> readGroupInfos;

    public Map<Short, ReadGroupInfo> getReadGroupInfos() {
        return readGroupInfos;
    }

    public void setReadGroupInfos(Map<Short, ReadGroupInfo> readGroupInfos) {
        this.readGroupInfos = readGroupInfos;
    }

    public void reduce(GenomicLocationWithQuality key, Iterator<ReadPairInfo> values, OutputCollector<GenomicLocation, GMMScorerResults> output, Reporter reporter) throws IOException {
        if (log.isDebugEnabled() && key.chromosome == 0 && key.pos == 15000) {
            log.debug("reducing for key: " + key);
        }
        GMMScorerResults results = readPairInfoScorer.reduceReadPairInfos(values, readGroupInfos, key);
        if (log.isDebugEnabled() && key.chromosome == 0 && key.pos == 15000) {
            log.debug("got results : " + results);
        }
        output.collect(new GenomicLocation(key.chromosome, key.pos), results);
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

        readPairInfoScorer.setMaxLogMapqDiff(Double.parseDouble(job.get("max.log.mapq.diff")));
        readPairInfoScorer.setMinCoverage(Integer.parseInt(job.get("min.clean.coverage")));
    }
}

