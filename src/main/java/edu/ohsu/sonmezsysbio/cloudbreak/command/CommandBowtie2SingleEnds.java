package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.Bowtie2SingleEndMapper;
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
@Parameters(separators = "=", commandDescription = "Run a bowtie2 alignment in single ended mode")
public class CommandBowtie2SingleEnds extends BaseCloudbreakCommand {

    @Parameter(names = {"--HDFSDataDir"}, required = true, description = "HDFS directory that holds the read data")
    String hdfsDataDir;

    @Parameter(names = {"--HDFSAlignmentsDir"}, required = true, description = "HDFS directory to hold the alignment data")
    String hdfsAlignmentsDir;

    @Parameter(names = {"--reference"}, required = true, description = "HDFS path to the bowtie 2 fasta reference file")
    String reference;
    
    @Parameter(names = {"--numReports"}, required = true, description = "Max number of alignment hits to report with the -k option")
    String numReports;

    @Parameter(names = {"--HDFSPathToBowtieAlign"}, required = true, description = "HDFS path to the bowtie2 executable")
    String pathToBowtie2;

    public void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("Bowtie2 Single End Alignment");
        conf.setJarByClass(Cloudbreak.class);
        FileInputFormat.addInputPath(conf, new Path(hdfsDataDir));
        Path outputDir = new Path(hdfsAlignmentsDir);
        FileSystem.get(conf).delete(outputDir);
        FileOutputFormat.setOutputPath(conf, outputDir);

        conf.set("bowtie2.reference", reference);
        addDistributedCacheFile(conf, reference + ".1.bt2");
        addDistributedCacheFile(conf, reference + ".2.bt2");
        addDistributedCacheFile(conf, reference + ".3.bt2");
        addDistributedCacheFile(conf, reference + ".4.bt2");
        addDistributedCacheFile(conf, reference + ".rev.1.bt2");
        addDistributedCacheFile(conf, reference + ".rev.2.bt2");


        addDistributedCacheFile(conf, pathToBowtie2, "bowtie2.executable");

        DistributedCache.createSymlink(conf);
        conf.set("mapred.task.timeout", "3600000");
        conf.set("bowtie2.num.reports", numReports);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setMapperClass(Bowtie2SingleEndMapper.class);
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
