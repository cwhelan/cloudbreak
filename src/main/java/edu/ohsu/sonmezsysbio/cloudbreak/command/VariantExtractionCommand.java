package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.file.FaidxFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.file.WigFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.GMMResultsToChromosomeMapper;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GMMResultsToVariantCallsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 1/31/13
 * Time: 10:01 AM
 */
public abstract class VariantExtractionCommand extends BaseCloudbreakCommand {
    @Parameter(names = {"--targetIsize"}, required = true)
    int targetIsize;
    @Parameter(names = {"--targetIsizeSD"}, required = true)
    int targetIsizeSD;
    @Parameter(names = {"--faidx"}, required = true)
    String faidxFileName;
    @Parameter(names = {"--threshold"})
    Double threshold = 1.68;
    @Parameter(names = {"--medianFilterWindow"})
    int medianFilterWindow = 5;
    @Parameter(names = {"--outputHDFSDir"}, required = true)
    String outputHDFSDir;
    @Parameter(names = {"--inputHDFSDir"}, required = true)
    String inputHDFSDir;
    @Parameter(names = {"--resolution"})
    int resolution = Cloudbreak.DEFAULT_RESOLUTION;
    @Parameter(names = {"--legacyAlignments"})
    boolean legacyAlignments = false;


    protected abstract String getVariantType();

    protected int getNumChromosomes(FileSystem dfs, String faidxFileName1) throws IOException {
        Path faidxPath = new Path(faidxFileName1);
        FSDataInputStream is = dfs.open(faidxPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        int chromosomes = 0;
        while (reader.readLine() != null) {
            chromosomes++;
        }
        return chromosomes;
    }

    protected void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("Extract variant calls (" + getVariantType() +")");
        conf.setJarByClass(Cloudbreak.class);

        FileSystem dfs = DistributedFileSystem.get(conf);

        FileInputFormat.addInputPath(conf, new Path(inputHDFSDir));
        Path outputDir = new Path(outputHDFSDir);

        FileSystem.get(conf).delete(outputDir);
        FileOutputFormat.setOutputPath(conf, outputDir);

        // get the number of chromosomes and set the number of reducers to it
        int chromosomes = getNumChromosomes(dfs, faidxFileName);
        conf.set("mapred.reduce.tasks", String.valueOf(chromosomes + 1));

        addDistributedCacheFile(conf, faidxFileName, "alignment.faidx");

        configureParams(conf);

        DistributedCache.createSymlink(conf);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setMapperClass(GMMResultsToChromosomeMapper.class);
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setReducerClass(GMMResultsToVariantCallsReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setCompressMapOutput(true);

        JobClient.runJob(conf);

    }

    protected void configureParams(JobConf conf) {
        conf.set("legacy.alignments", String.valueOf(legacyAlignments));
        conf.set("target.isize", String.valueOf(targetIsize));
        conf.set("target.isizesd", String.valueOf(targetIsizeSD));
        conf.set("variant.lr.threshold", String.valueOf(threshold));
        conf.set("variant.mfw", String.valueOf(medianFilterWindow));
        conf.set("cloudbreak.resolution", String.valueOf(resolution));
        conf.set("variant.type", getVariantType());
    }
}
