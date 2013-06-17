package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.cloud.CloudCluster;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/30/13
 * Time: 12:17 PM
 */
@Parameters(commandDescription = "Execute a script on one node of the currently running cloud cluster")
public class CommandRunScriptOnCluster implements CloudbreakCommand {

    @Parameter(names = "--fileName", required = true, description = "Path on the local filesystem of the script to run")
    String fileName;

    @Override
    public void run(Configuration conf) throws Exception {
        CloudCluster cluster = new CloudCluster();
        cluster.runScript(fileName);
    }
}
