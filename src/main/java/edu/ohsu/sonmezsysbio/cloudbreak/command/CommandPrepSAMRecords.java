package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.SingleEndAlignmentSummaryMapper;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.SingleEndAlignmentSummaryReducer;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.SingleEndAlignmentsToPairsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/16/12
 * Time: 1:39 PM
 *
 * Takes some SAM Records that have been loaded into HDFS and sorts and arranges them so that they
 * are in the Cloudbreak paired alignment format.
 */
public class CommandPrepSAMRecords implements CloudbreakCommand {
    @Parameter(names = {"--inputHDFSDir"}, required = true)
    String inputHDFSDir;

    @Parameter(names = {"--outputHDFSDir"}, required = true)
    String outputHDFSDir;

    public void run(Configuration conf) throws Exception {
        runHadoopJob(conf);
    }

    private void runHadoopJob(Configuration configuration) throws IOException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("Prep Sam Records");
        conf.setJarByClass(Cloudbreak.class);
        FileInputFormat.addInputPath(conf, new Path(inputHDFSDir));

        Path outputDir = new Path(outputHDFSDir);
        FileSystem.get(conf).delete(outputDir);
        FileOutputFormat.setOutputPath(conf, outputDir);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setMapperClass(IdentityMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setReducerClass(SingleEndAlignmentsToPairsReducer.class);

        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.set("cloudbreak.aligner", Cloudbreak.ALIGNER_GENERIC_SAM);

        conf.setCompressMapOutput(true);
        conf.set("mapred.output.compress","true");
        conf.set("mapred.output.compression","org.apache.hadoop.io.compress.SnappyCodec");

        JobClient.runJob(conf);

    }
}
