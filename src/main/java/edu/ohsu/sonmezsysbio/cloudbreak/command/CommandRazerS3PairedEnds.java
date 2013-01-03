package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.RazerS3PairedEndMapper;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.PairedEndSAMAlignmentsToPairsReducer;
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
 * Date: 5/18/11
 * Time: 2:01 PM
 */
@Parameters(separators = "=", commandDescription = "Run a razerS3 alignment")
public class CommandRazerS3PairedEnds extends BaseCloudbreakCommand {

    @Parameter(names = {"--HDFSDataDir"}, required = true)
    String hdfsDataDir;

    @Parameter(names = {"--HDFSAlignmentsDir"}, required = true)
    String hdfsAlignmentsDir;

    @Parameter(names = {"--reference"}, required = true)
    String reference;
    
    @Parameter(names = {"--numReports"}, required = true)
    String numReports;

    @Parameter(names = {"--HDFSPathToRazerS3"}, required = true)
    String pathToRazerS3;

    @Parameter(names = {"--maxInsertSize"}, required = true)
    int targetIsize;

    public void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("RazerS3 Paired End Alignment");
        conf.setJarByClass(Cloudbreak.class);
        FileInputFormat.addInputPath(conf, new Path(hdfsDataDir));
        Path outputDir = new Path(hdfsAlignmentsDir);
        FileSystem.get(conf).delete(outputDir);
        FileOutputFormat.setOutputPath(conf, outputDir);

        conf.set("razers.reference", reference);
        addDistributedCacheFile(conf, reference);


        addDistributedCacheFile(conf, pathToRazerS3, "razers.executable");

        DistributedCache.createSymlink(conf);
        conf.set("mapred.task.timeout", "3600000");
        conf.set("razers.num.reports", numReports);

        conf.set("razers.maxIsize", String.valueOf(targetIsize));

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setMapperClass(RazerS3PairedEndMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setCompressMapOutput(true);

        conf.setReducerClass(PairedEndSAMAlignmentsToPairsReducer.class);
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
