package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.MrFastSingleEndMapper;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.SingleEndAlignmentsToPairsReducer;
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
 * Date: 6/17/12
 * Time: 2:01 PM
 */
@Parameters(separators = "=", commandDescription = "Run a novoalign mate pair alignment")
public class CommandMrFastSingleEnds extends BaseCloudbreakCommand {

    @Parameter(names = {"--HDFSDataDir"}, required = true)
    String hdfsDataDir;

    @Parameter(names = {"--HDFSAlignmentsDir"}, required = true)
    String hdfsAlignmentsDir;

    @Parameter(names = {"--reference"}, required = true)
    String reference;

    @Parameter(names = {"--HDFSPathToMrfast"}, required = true)
    String pathToMrfast;

    @Parameter(names = {"--threshold"})
    int threshold = -1;

    public void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("Single End Alignment");
        conf.setJarByClass(Cloudbreak.class);
        FileInputFormat.addInputPath(conf, new Path(hdfsDataDir));
        Path outputDir = new Path(hdfsAlignmentsDir);
        FileSystem.get(conf).delete(outputDir);

        FileOutputFormat.setOutputPath(conf, outputDir);

        conf.setInputFormat(TextInputFormat.class);

        addDistributedCacheFile(conf, pathToMrfast, "mrfast.executable");
        addDistributedCacheFile(conf, reference, "mrfast.reference");
        addDistributedCacheFile(conf, reference + ".index", "mrfast.index");

        DistributedCache.createSymlink(conf);
        conf.set("mapred.task.timeout", "3600000");

        if (threshold != -1) {
            conf.set("mrfast.threshold", String.valueOf(threshold));
        }

        conf.setMapperClass(MrFastSingleEndMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setCompressMapOutput(true);

        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.set("mapred.output.compress", "true");
        conf.set("mapred.output.compression", "org.apache.hadoop.io.compress.SnappyCodec");

        conf.setReducerClass(SingleEndAlignmentsToPairsReducer.class);

        JobClient.runJob(conf);

    }

    public void run(Configuration conf) throws Exception {
        runHadoopJob(conf);
    }
}
