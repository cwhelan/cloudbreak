package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocation;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GMMScorerResults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 12/14/12
 * Time: 12:11 PM
 */

@Parameters(separators = "=", commandDescription = "Sort and merge GMM Results (use with one reducer to get all GMM feature results into a single file")
public class CommandSortGMMResults extends BaseCloudbreakCommand {

    @Parameter(names = {"--inputHDFSDir"}, required = true, description = "HDFS path to the directory holding the GMM features")
    String inputHDFSDir;

    @Parameter(names = {"--outputHDFSDir"}, required = true, description = "Directory in which to put the results")
    String outputHDFSDir;

    public void run(Configuration conf) throws Exception {
        JobConf jobConf = new JobConf(conf);

        jobConf.setJobName("Sort and merge GMM Results");
        jobConf.setJarByClass(Cloudbreak.class);

        FileInputFormat.addInputPath(jobConf, new Path(inputHDFSDir));
        Path outputDirPath = new Path(outputHDFSDir);

        FileSystem.get(jobConf).delete(outputDirPath);
        FileOutputFormat.setOutputPath(jobConf, outputDirPath);

        jobConf.setInputFormat(SequenceFileInputFormat.class);

        jobConf.setOutputKeyClass(GenomicLocation.class);
        jobConf.setOutputValueClass(GMMScorerResults.class);
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);

        jobConf.setMapperClass(IdentityMapper.class);

        jobConf.setMapOutputKeyClass(GenomicLocation.class);
        jobConf.setMapOutputValueClass(GMMScorerResults.class);

        jobConf.setReducerClass(IdentityReducer.class);

        jobConf.setOutputKeyClass(GenomicLocation.class);
        jobConf.setOutputValueClass(GMMScorerResults.class);
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);

        jobConf.set("mapred.output.compress", "true");
        jobConf.set("mapred.output.compression", "org.apache.hadoop.io.compress.SnappyCodec");

        JobClient.runJob(jobConf);

    }
}
