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
@Parameters(separators = "=", commandDescription = "Run a BWA paired-end alignment")
public class CommandBWAPairedEnds extends BaseCloudbreakCommand {
    @Parameter(names = {"--HDFSDataDir"}, required = true, description = "HDFS directory that holds the read data")
    String hdfsDataDir;

    @Parameter(names = {"--HDFSAlignmentsDir"}, required = true, description = "HDFS directory to hold the alignment data")
    String hdfsAlignmentsDir;

    @Parameter(names = {"--referenceBasename"}, required = true, description = "HDFS path of the FASTA file from which the BWA index files were generated.")
    String reference;

    @Parameter(names = {"--numExtraReports"}, description = "If > 0, set -n and -N params to bwa sampe, and use xa2multi.pl to report multiple hits")
    String numExtraReports = "0";

    @Parameter(names = {"--HDFSPathToBWA"}, required = true, description = "HDFS path to the bwa executable")
    String pathToBWA;

    @Parameter(names = {"--HDFSPathToXA2multi"}, description = "HDFS path to the bwa xa2multi.pl executable")
    String pathToXA2multi;

    @Parameter(names = {"--maxProcessesOnNode"}, required = true, description = "Ensure that only a max of this many BWA processes are running on each node at once.")
    int maxProcessesOnNode = 6;

    public void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        if (Integer.parseInt(numExtraReports) != 0 && pathToXA2multi == null) {
            throw new IllegalArgumentException("Need to specify path to xa2multi.pl if numExtraReports is not 0");
        }

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
