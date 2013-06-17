package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.ReadGroupInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.file.DFSFacade;
import edu.ohsu.sonmezsysbio.cloudbreak.file.ReadGroupInfoFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.AlignmentsToBedSpansMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/23/11
 * Time: 10:02 AM
 */
@Parameters(separators = "=", commandDescription = "Dump all read pairs that span the given region with their deletion scores to BED format (debugging)")
public class CommandDumpReadsWithScores extends BaseCloudbreakCommand {

    @Parameter(names = {"--inputFileDescriptor"}, required = true, description = "HDFS path to the directory that holds the alignment records")
    String inputFileDescriptor;

    @Parameter(names = {"--outputHDFSDir"}, required = true, description = "HDFS path to the directory that will hold the output")
    String outputHDFSDir;

    @Parameter(names = {"--region"}, required = true, description = "region to find read pairs for, in chr:start-end format")
    String region;

    @Parameter(names = {"--maxInsertSize"}, description = "Maximum possible insert size to consider")
    int maxInsertSize = 500000;

    @Parameter(names = {"--aligner"}, description="Format of the alignment records (" + Cloudbreak.ALIGNER_GENERIC_SAM + "|" + Cloudbreak.ALIGNER_MRFAST + "|" + Cloudbreak.ALIGNER_NOVOALIGN + ")")
    String aligner = Cloudbreak.ALIGNER_GENERIC_SAM;

    @Parameter(names = {"--minScore"}, description = "Minimum alignment score (SAM tag AS); all reads with lower AS will be ignored")
    int minScore = -1;

    @Parameter(names = {"--stripChromosomeNamesAtWhitespace"}, description = "Clip chromosome names from the reference at the first whitespace so they match with alignment fields")
    boolean stripChromosomeNamesAtWhitespace = false;

    public void run(Configuration configuration) throws IOException, URISyntaxException {
        runHadoopJob(configuration);
    }

    private void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("Debug alignment spans");
        conf.setJarByClass(Cloudbreak.class);

        ReadGroupInfoFileHelper readGroupInfoFileHelper = new ReadGroupInfoFileHelper();
        FileSystem dfs = DistributedFileSystem.get(conf);

        Map<Short, ReadGroupInfo> readGroupInfoMap = readGroupInfoFileHelper.readReadGroupsById(
                new BufferedReader(
                        new InputStreamReader(
                                new DFSFacade(dfs, conf).openPath(new Path(inputFileDescriptor)))));

        for (ReadGroupInfo readGroupInfo : readGroupInfoMap.values()) {
            FileInputFormat.addInputPath(conf, new Path(readGroupInfo.hdfsPath));
        }

        Path outputDir = new Path(outputHDFSDir);
        FileSystem.get(conf).delete(outputDir);

        FileOutputFormat.setOutputPath(conf, outputDir);

        conf.setInputFormat(SequenceFileInputFormat.class);

        addDistributedCacheFile(conf, inputFileDescriptor, "read.group.info.file");
        DistributedCache.createSymlink(conf);

        conf.set("cloudbreak.aligner", aligner);
        conf.set("pileupDeletionScore.maxInsertSize", String.valueOf(maxInsertSize));
        conf.set("pileupDeletionScore.region", region);

        conf.set("pileupDeletionScore.minScore", String.valueOf(minScore));

        conf.set("alignments.strip.chromosome.name.at.whitespace", String.valueOf(stripChromosomeNamesAtWhitespace));

        conf.setMapperClass(AlignmentsToBedSpansMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setReducerClass(IdentityReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        JobClient.runJob(conf);

    }
}
