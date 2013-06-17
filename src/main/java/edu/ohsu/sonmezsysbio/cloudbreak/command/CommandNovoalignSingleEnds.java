package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.NovoalignSingleEndMapper;
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
 * Date: 5/18/11
 * Time: 2:01 PM
 */
@Parameters(separators = "=", commandDescription = "Run a Novoalign alignment in single ended mode")
public class CommandNovoalignSingleEnds extends BaseCloudbreakCommand {

    @Parameter(names = {"--HDFSDataDir"}, required = true, description = "HDFS directory that holds the read data")
    String hdfsDataDir;

    @Parameter(names = {"--HDFSAlignmentsDir"}, required = true, description = "HDFS directory to hold the alignment data")
    String hdfsAlignmentsDir;

    @Parameter(names = {"--reference"}, required = true, description = "HDFS path to the Novoalign reference index file")
    String reference;
    
    @Parameter(names = {"--threshold"}, required = true, description = "Quality threshold to use for the -t parameter")
    String threshold;

    @Parameter(names = {"--qualityFormat"}, description = "Quality score format of the FASTQ files")
    String qualityFormat = "ILMFQ";

    @Parameter(names = {"--HDFSPathToNovoalign"}, required = true, description = "HDFS path to the Novoalign executable")
    String pathToNovoalign;

    @Parameter(names = {"--HDFSPathToNovoalignLicense"}, description = "HDFS path to the Novoalign license filez")
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
