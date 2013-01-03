package edu.ohsu.sonmezsysbio.cloudbreak.command;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 7/27/12
 * Time: 11:26 AM
 */
public abstract class BaseCloudbreakCommand implements CloudbreakCommand {

    protected String addDistributedCacheFile(JobConf conf, String hdfsPath) throws URISyntaxException {
        return addDistributedCacheFile(conf, hdfsPath, null);
    }

    protected String addDistributedCacheFile(JobConf conf, String hdfsPath, String basenameConfigProperty) throws URISyntaxException {
        File file = new File(hdfsPath);
        String basename = file.getName();
        String dir = file.getParent();

        DistributedCache.addCacheFile(new URI(dir + "/" + basename + "#" + basename),
                conf);

        if (basenameConfigProperty != null) {
            conf.set(basenameConfigProperty, basename);
        }
        return basename;
    }
}
