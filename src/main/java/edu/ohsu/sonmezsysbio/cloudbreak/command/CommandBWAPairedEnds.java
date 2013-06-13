package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.BWAPairedEndMapper;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.AlignmentsToPairsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 6/12/13
 * Time: 3:23 PM
 */
@Parameters(separators = "=", commandDescription = "Run a BWA alignment")
public class CommandBWAPairedEnds extends BaseCloudbreakCommand {
    @Parameter(names = {"--HDFSDataDir"}, required = true)
    String hdfsDataDir;

    @Parameter(names = {"--HDFSAlignmentsDir"}, required = true)
    String hdfsAlignmentsDir;

    @Parameter(names = {"--referenceBasename"}, required = true)
    String reference;

    @Parameter(names = {"--numExtraReports"}, required = true)
    String numExtraReports = "0";

    @Parameter(names = {"--HDFSPathToBWA"}, required = true)
    String pathToBWA;

    @Parameter(names = {"--HDFSPathToXA2multi"})
    String pathToXA2multi;

    @Parameter(names = {"--maxProcessesOnNode"}, required = true)
    int maxProcessesOnNode = 6;

    public void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("BWA Paired End Alignment");
        conf.setJarByClass(Cloudbreak.class);
        FileInputFormat.addInputPath(conf, new Path(hdfsDataDir));
        Path outputDir = new Path(hdfsAlignmentsDir);
        FileSystem.get(conf).delete(outputDir);
        FileOutputFormat.setOutputPath(conf, outputDir);

        conf.set("bwa.reference", reference);
        addDistributedCacheFile(conf, reference + ".amb");
        addDistributedCacheFile(conf, reference + ".ann");
        addDistributedCacheFile(conf, reference + ".bwt");
        addDistributedCacheFile(conf, reference + ".pac");
        addDistributedCacheFile(conf, reference + ".sa");

        addDistributedCacheFile(conf, pathToBWA, "bwa.executable");
        addDistributedCacheFile(conf, pathToXA2multi, "bwa.xa2multi");

        DistributedCache.createSymlink(conf);
        conf.set("mapred.task.timeout", "3600000");
        conf.set("bwa.num.extra.reports", numExtraReports);

        conf.set("bwa.max.processes.on.node", String.valueOf(maxProcessesOnNode));

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setMapperClass(BWAPairedEndMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setCompressMapOutput(true);

        conf.setReducerClass(AlignmentsToPairsReducer.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.set("mapred.output.compress","true");
        conf.set("mapred.output.compression","org.apache.hadoop.io.compress.SnappyCodec");

        JobClient.runJob(conf);

    }

    public void run(Configuration conf) throws Exception {
        runHadoopJob(conf);
    }

}
