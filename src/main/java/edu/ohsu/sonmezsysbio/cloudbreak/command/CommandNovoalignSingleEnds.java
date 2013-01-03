package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.SingleEndAlignmentsToPairsReducer;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.NovoalignSingleEndMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/18/11
 * Time: 2:01 PM
 */
@Parameters(separators = "=", commandDescription = "Run a novoalign mate pair alignment")
public class CommandNovoalignSingleEnds extends BaseCloudbreakCommand {

    @Parameter(names = {"--HDFSDataDir"}, required = true)
    String hdfsDataDir;

    @Parameter(names = {"--HDFSAlignmentsDir"}, required = true)
    String hdfsAlignmentsDir;

    @Parameter(names = {"--reference"}, required = true)
    String reference;
    
    @Parameter(names = {"--threshold"}, required = true)
    String threshold;

    @Parameter(names = {"--qualityFormat"})
    String qualityFormat = "ILMFQ";

    @Parameter(names = {"--HDFSPathToNovoalign"}, required = true)
    String pathToNovoalign;

    @Parameter(names = {"--HDFSPathToNovoalignLicense"})
    String pathToNovoalignLicense;

    public void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("Single End Alignment");
        conf.setJarByClass(Cloudbreak.class);
        FileInputFormat.addInputPath(conf, new Path(hdfsDataDir));
        Path outputDir = new Path(hdfsAlignmentsDir);
        FileSystem.get(conf).delete(outputDir);
        FileOutputFormat.setOutputPath(conf, outputDir);

        addDistributedCacheFile(conf, reference, "novoalign.reference");

        addDistributedCacheFile(conf, pathToNovoalign, "novoalign.executable");
        if (pathToNovoalignLicense != null) {
            addDistributedCacheFile(conf, pathToNovoalignLicense, "novoalign.license");
        }

        DistributedCache.createSymlink(conf);
        conf.set("mapred.task.timeout", "3600000");
        conf.set("novoalign.threshold", threshold);
        conf.set("novoalign.quality.format", qualityFormat);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setMapperClass(NovoalignSingleEndMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setCompressMapOutput(true);

        conf.setReducerClass(SingleEndAlignmentsToPairsReducer.class);
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
