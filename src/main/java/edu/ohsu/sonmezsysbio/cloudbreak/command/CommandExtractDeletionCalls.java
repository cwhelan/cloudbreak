package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.GMMResultsToChromosomeMapper;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GMMResultsToDeletionCallsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/22/12
 * Time: 10:33 PM
 */
@Parameters(separators = "=", commandDescription = "Extract deletion calls into a BED file")
public class CommandExtractDeletionCalls extends VariantExtractionCommand implements CloudbreakCommand {

    @Parameter(names = {"--outputHDFSDir"}, required = true)
    String outputHDFSDir;
    @Parameter(names = {"--inputHDFSDir"}, required = true)
    String inputHDFSDir;
    @Parameter(names = {"--resolution"})
    int resolution = Cloudbreak.DEFAULT_RESOLUTION;

    @Parameter(names = {"--legacyAlignments"})
    boolean legacyAlignments = false;

    @Override
    protected String getVariantType() {
        return Cloudbreak.VARIANT_TYPE_DELETION;
    }

    @Override
    public void run(Configuration conf) throws Exception {
        runHadoopJob(conf);
    }

    private void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("Extract deletion calls");
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

        conf.set("legacy.alignments", String.valueOf(legacyAlignments));
        conf.set("target.isize", String.valueOf(targetIsize));
        conf.set("target.isizesd", String.valueOf(targetIsizeSD));
        conf.set("variant.lr.threshold", String.valueOf(threshold));
        conf.set("variant.mfw", String.valueOf(medianFilterWindow));

        DistributedCache.createSymlink(conf);

        conf.set("cloudbreak.resolution", String.valueOf(resolution));

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setMapperClass(GMMResultsToChromosomeMapper.class);
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setReducerClass(GMMResultsToDeletionCallsReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setCompressMapOutput(true);

        JobClient.runJob(conf);

    }

    private int getNumChromosomes(FileSystem dfs, String faidxFileName1) throws IOException {
        Path faidxPath = new Path(faidxFileName1);
        FSDataInputStream is = dfs.open(faidxPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        int chromosomes = 0;
        while (reader.readLine() != null) {
            chromosomes++;
        }
        return chromosomes;
    }

}
