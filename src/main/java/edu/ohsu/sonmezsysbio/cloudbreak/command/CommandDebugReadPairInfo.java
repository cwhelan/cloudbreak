package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.ReadGroupInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.file.DFSFacade;
import edu.ohsu.sonmezsysbio.cloudbreak.file.ReadGroupInfoFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocationWithQuality;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocationWithQualityGroupingComparator;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocationWithQualitySortComparator;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.AlignmentsToReadPairInfoMapper;
import edu.ohsu.sonmezsysbio.cloudbreak.partitioner.GenomicLocationWithQualityPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
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
 * Date: 4/9/12
 * Time: 11:12 AM
 */
@Parameters(separators = "=", commandDescription = "Compute the raw data that goes into the GMM fit procedure for each bin (use with filter to debug a particular locus)")
public class CommandDebugReadPairInfo extends BaseCloudbreakCommand {

    @Parameter(names = {"--inputFileDescriptor"}, required = true, description = "HDFS path to the directory that holds the alignment records")
    String inputFileDescriptor;

    @Parameter(names = {"--outputHDFSDir"}, required = true, description = "HDFS directory to hold the output")
    String ouptutHDFSDir;

    @Parameter(names = {"--maxInsertSize"}, description = "Maximum insert size to consider (= max size of deletion detectable)")
    int maxInsertSize = 500000;

    @Parameter(names = {"--faidx"}, required=true, description = "HDFS path to the chromosome length file for the reference genome")
    String faidxFileName;

    @Parameter(names = {"--chrFilter"}, required = true, description = "Print info for alignments in the region chrFilter:startFilter-endFilter")
    String chrFilter;

    @Parameter(names = {"--startFilter"}, required = true, description = "see chrFilter")
    Long startFilter;

    @Parameter(names = {"--endFilter"}, required = true, description = "see chrFilter")
    Long endFilter;

    @Parameter(names = {"--resolution"}, description = "Size of the bins to tile the genome with")
    int resolution = Cloudbreak.DEFAULT_RESOLUTION;

    @Parameter(names = {"--excludePairsMappingIn"}, description = "HDFS path to a BED file. Any reads mapped within those intervals will be excluded from the processing")
    String exclusionRegionsFileName;

    @Parameter(names = {"--mapabilityWeighting"}, description = "HDFS path to a BigWig file containing genome uniqness scores. If specified, Cloudbreak will weight reads by the uniqueness of the regions they mapped to")
    String mapabilityWeightingFileName;

    @Parameter(names = {"--aligner"}, description="Format of the alignment records (" + Cloudbreak.ALIGNER_GENERIC_SAM + "|" + Cloudbreak.ALIGNER_MRFAST + "|" + Cloudbreak.ALIGNER_NOVOALIGN + ")")
    String aligner = Cloudbreak.ALIGNER_GENERIC_SAM;

    @Parameter(names = {"--minScore"}, description = "Minimum alignment score (SAM tag AS); all reads with lower AS will be ignored")
    int minScore = -1;

    public void run(Configuration conf) throws Exception {
        runHadoopJob(conf);
    }

    private void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("Debug Read Pair Info");
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
        Path outputDir = new Path(ouptutHDFSDir);
        FileSystem.get(conf).delete(outputDir);

        FileOutputFormat.setOutputPath(conf, outputDir);

        addDistributedCacheFile(conf, inputFileDescriptor, "read.group.info.file");
        addDistributedCacheFile(conf, faidxFileName, "alignment.faidx");

        if (exclusionRegionsFileName != null) {
            addDistributedCacheFile(conf, exclusionRegionsFileName, "alignment.exclusionRegions");
        }

        if (mapabilityWeightingFileName != null) {
            addDistributedCacheFile(conf, mapabilityWeightingFileName, "alignment.mapabilityWeighting");
        }

        DistributedCache.createSymlink(conf);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.set("cloudbreak.resolution", String.valueOf(resolution));
        conf.set("cloudbreak.aligner", aligner);

        conf.set("pileupDeletionScore.maxInsertSize", String.valueOf(maxInsertSize));
        conf.set("alignments.filterchr", chrFilter);
        conf.set("alignments.filterstart", startFilter.toString());
        conf.set("alignments.filterend", endFilter.toString());

        conf.set("pileupDeletionScore.minScore", String.valueOf(minScore));

        conf.setMapperClass(AlignmentsToReadPairInfoMapper.class);
        conf.setMapOutputKeyClass(GenomicLocationWithQuality.class);
        conf.setMapOutputValueClass(ReadPairInfo.class);
        conf.setOutputKeyComparatorClass(GenomicLocationWithQualitySortComparator.class);
        conf.setOutputValueGroupingComparator(GenomicLocationWithQualityGroupingComparator.class);
        conf.setPartitionerClass(GenomicLocationWithQualityPartitioner.class);


        conf.setReducerClass(IdentityReducer.class);

        conf.setOutputKeyClass(GenomicLocationWithQuality.class);
        conf.setOutputValueClass(ReadPairInfo.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        JobClient.runJob(conf);

    }
}
