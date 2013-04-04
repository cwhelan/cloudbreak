package edu.ohsu.sonmezsysbio.cloudbreak.command;

import edu.ohsu.sonmezsysbio.cloudbreak.cloud.CloudCluster;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/29/13
 * Time: 2:33 PM
 */
public class CommandLaunchCluster implements CloudbreakCommand {

    @Override
    public void run(Configuration conf) throws Exception {
        CloudCluster cluster = new CloudCluster();
        cluster.launch();


//        System.err.println("Starting local SOCKS proxy");
//        proxy = new HadoopProxy(clusterSpec, cluster);
//        proxy.start();

        /**
         * Obtain a Hadoop configuration object and wait for services to start
         */
//        Configuration config = getHadoopConfiguration(cluster);
//        JobConf job = new JobConf(config, HadoopClusterExample.class);
//        JobClient client = new JobClient(job);
//
//        waitToExitSafeMode(client);
//        waitForTaskTrackers(client);
    }
}
