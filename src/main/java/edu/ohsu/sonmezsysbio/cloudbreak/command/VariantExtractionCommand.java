package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.io.*;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.GMMResultsToChromosomeMapper;
import edu.ohsu.sonmezsysbio.cloudbreak.partitioner.GenomicLocationChromosomePartitioner;
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

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 1/31/13
 * Time: 10:01 AM
 */
public abstract class VariantExtractionCommand extends BaseCloudbreakCommand {
    @Parameter(names = {"--targetIsize"}, required = true, description="Mean insert size of the library")
    int targetIsize;
    @Parameter(names = {"--targetIsizeSD"}, required = true, description = "Standard deviation of the insert size of the library")
    int targetIsizeSD;
    @Parameter(names = {"--faidx"}, required = true, description = "Chromosome length file for the reference")
    String faidxFileName;
    @Parameter(names = {"--threshold"}, description = "Likelihood ratio threshold to call a variant")
    Double threshold = 1.68;
    @Parameter(names = {"--medianFilterWindow"}, description = "Use a median filter of this size to clean up the results")
    int medianFilterWindow = 5;
    @Parameter(names = {"--outputHDFSDir"}, required = true, description = "HDFS Directory to store the variant calls in")
    String outputHDFSDir;
    @Parameter(names = {"--inputHDFSDir"}, required = true, description = "HDFS path to the GMM fit feature results")
    String inputHDFSDir;
    @Parameter(names = {"--resolution"}, description = "Size of the bins to tile the genome with")
    int resolution = Cloudbreak.DEFAULT_RESOLUTION;


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
        conf.setMapOutputKeyClass(GenomicLocation.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setPartitionerClass(GenomicLocationChromosomePartitioner.class);
        conf.setOutputValueGroupingComparator(GenomicLocationChromosomeGroupingComparator.class);
        conf.setReducerClass(GMMResultsToVariantCallsReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setCompressMapOutput(true);

        JobClient.runJob(conf);

    }

    protected void configureParams(JobConf conf) {
        conf.set("target.isize", String.valueOf(targetIsize));
        conf.set("target.isizesd", String.valueOf(targetIsizeSD));
        conf.set("variant.lr.threshold", String.valueOf(threshold));
        conf.set("variant.mfw", String.valueOf(medianFilterWindow));
        conf.set("cloudbreak.resolution", String.valueOf(resolution));
        conf.set("variant.type", getVariantType());
    }
}
