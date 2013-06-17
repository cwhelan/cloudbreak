package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.GEMSingleEndMapper;
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
@Parameters(separators = "=", commandDescription = "Run a GEM alignment")
public class CommandGEMSingleEnds extends BaseCloudbreakCommand {

    @Parameter(names = {"--HDFSDataDir"}, required = true, description = "HDFS directory that holds the read data")
    String hdfsDataDir;

    @Parameter(names = {"--HDFSAlignmentsDir"}, required = true, description = "HDFS directory to hold the alignment data")
    String hdfsAlignmentsDir;

    @Parameter(names = {"--reference"}, required = true, description = "HDFS path to the GEM reference file")
    String reference;

    @Parameter(names = {"--numReports"}, required = true, description = "Max number of hits to report from GEM")
    String numReports;

    @Parameter(names = {"--HDFSPathToGEMMapper"}, required = true, description = "HDFS path to the gem-mapper executable")
    String pathToGEMMapper;

    @Parameter(names = {"--HDFSPathToGEM2SAM"}, required = true, description = "HDFS path to the gem-2-sam executable")
    String pathToGEM2SAM;

    @Parameter(names = {"--editDistance"}, required = true, description = "Edit distance parameter (-e) to use in the GEM mapping")
    int editDistance;

    @Parameter(names = {"--strata"}, description = "Strata parameter (-s) to use in the GEM mapping")
    String strata = "all";

    @Parameter(names = {"--maxProcessesOnNode"}, required = true, description = "Maximum number of GEM mapping processes to run on one node simultaneously")
    int maxProcessesOnNode = 6;

    public void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("GEM Mapper Single End Alignment");
        conf.setJarByClass(Cloudbreak.class);
        FileInputFormat.addInputPath(conf, new Path(hdfsDataDir));
        Path outputDir = new Path(hdfsAlignmentsDir);
        FileSystem.get(conf).delete(outputDir);
        FileOutputFormat.setOutputPath(conf, outputDir);

        addDistributedCacheFile(conf, reference, "gem.reference");
        addDistributedCacheFile(conf, pathToGEMMapper, "gem.mapper.executable");
        addDistributedCacheFile(conf, pathToGEM2SAM, "gem.tosam.executable");

        DistributedCache.createSymlink(conf);
        conf.set("mapred.task.timeout", "3600000");
        conf.set("gem.num.reports", numReports);
        conf.set("gem.edit.distance", String.valueOf(editDistance));
        conf.set("gem.strata", strata);
        conf.set("gem.max.processes.on.node", String.valueOf(maxProcessesOnNode));

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setMapperClass(GEMSingleEndMapper.class);
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
