package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocation;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.GenomicLocationScoreThresholdMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 7/3/13
 * Time: 4:43 PM
 */
public class CommandSplitSequenceFile extends BaseCloudbreakCommand {
    @Parameter(names = {"--inputFile"}, required = true)
    String inputFile;

    @Parameter(names = {"--outputHDFSDir"}, required = true)
    String outputHDFSDir;

    public void run(Configuration conf) throws Exception {
        runHadoopJob(conf);
    }

    private void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("Split sequence file");
        conf.setJarByClass(Cloudbreak.class);

        FileInputFormat.addInputPath(conf, new Path(inputFile));

        Path outputDir = new Path(outputHDFSDir);

        FileSystem.get(conf).delete(outputDir);

        FileOutputFormat.setOutputPath(conf, outputDir);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setMapperClass(IdentityMapper.class);

        conf.setReducerClass(IdentityReducer.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setCompressMapOutput(true);

        conf.set("mapred.output.compress","true");
        conf.set("mapred.output.compression","org.apache.hadoop.io.compress.SnappyCodec");

        JobClient.runJob(conf);

    }

}
