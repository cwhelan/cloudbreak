package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.CloudbreakMapReduceBase;
import edu.ohsu.sonmezsysbio.cloudbreak.ReadGroupInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.file.ReadGroupInfoFileHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 12/9/12
 * Time: 11:24 AM
 */
public class SingleEndAlignmentsMapper extends CloudbreakMapReduceBase {

    private static org.apache.log4j.Logger logger = Logger.getLogger(SingleEndAlignmentsMapper.class);

    protected boolean matePairs;
    protected Integer maxInsertSize = Cloudbreak.DEFAULT_MAX_INSERT_SIZE;
    protected double targetIsize;
    protected double targetIsizeSD;
    protected Short readGroupId;

    protected static String getInputPath(String mapInputProperty) {
        String path = new Path(mapInputProperty).toUri().getPath();
        return path;
    }

    public Short getReadGroupId() {
        return readGroupId;
    }

    public void setReadGroupId(Short readGroupId) {
        this.readGroupId = readGroupId;
    }

    public double getTargetIsize() {
        return targetIsize;
    }

    public void setTargetIsize(double targetIsize) {
        this.targetIsize = targetIsize;
    }

    public double getTargetIsizeSD() {
        return targetIsizeSD;
    }

    public void setTargetIsizeSD(double targetIsizeSD) {
        this.targetIsizeSD = targetIsizeSD;
    }

    protected void configureReadGroups(JobConf job) {
        // todo: if we change to the non-deprecated API, need to update this as described here
        // todo: https://issues.apache.org/jira/browse/MAPREDUCE-2166
        String inputFile = getInputPath(job.get("map.input.file"));

        String readGroupInfoFile = job.get("read.group.info.file");
        ReadGroupInfoFileHelper readGroupInfoFileHelper = new ReadGroupInfoFileHelper();
        Map<Short, ReadGroupInfo> readGroupInfos = null;
        try {
            readGroupInfos = readGroupInfoFileHelper.readReadGroupsById(readGroupInfoFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.debug("Looking up the read group for input file: " + inputFile);
        boolean configuredReadGroup = false;
        for (Short readGroupInfoId : readGroupInfos.keySet()) {
            logger.debug("comparing to: " + readGroupInfos.get(readGroupInfoId).hdfsPath);
            if (inputFile.startsWith(readGroupInfos.get(readGroupInfoId).hdfsPath)) {
                logger.debug("got it!");
                this.readGroupId = readGroupInfoId;
                ReadGroupInfo readGroupInfo = readGroupInfos.get(readGroupInfoId);
                matePairs = readGroupInfo.matePair;
                targetIsize = readGroupInfo.isize;
                targetIsizeSD = readGroupInfo.isizeSD;
                configuredReadGroup = true;
                break;
            }
        }
        if (! configuredReadGroup) throw new RuntimeException("Unable to configure read group for " + inputFile);
    }

    public boolean isMatePairs() {
        return matePairs;
    }

    public void setMatePairs(boolean matePairs) {
        this.matePairs = matePairs;
    }
}
