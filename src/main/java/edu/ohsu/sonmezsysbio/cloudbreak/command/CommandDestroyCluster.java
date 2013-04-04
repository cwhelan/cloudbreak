package edu.ohsu.sonmezsysbio.cloudbreak.command;

import edu.ohsu.sonmezsysbio.cloudbreak.cloud.CloudCluster;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/30/13
 * Time: 11:33 AM
 */
public class CommandDestroyCluster implements CloudbreakCommand {

    @Override
    public void run(Configuration conf) throws Exception {
        CloudCluster cluster = new CloudCluster();
        cluster.destroy();
    }
}
