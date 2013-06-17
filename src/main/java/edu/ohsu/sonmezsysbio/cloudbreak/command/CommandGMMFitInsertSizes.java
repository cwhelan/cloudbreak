package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.ReadGroupInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.file.DFSFacade;
import edu.ohsu.sonmezsysbio.cloudbreak.file.ReadGroupInfoFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.io.*;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.AlignmentsToReadPairInfoMapper;
import edu.ohsu.sonmezsysbio.cloudbreak.partitioner.GenomicLocationWithQualityPartitioner;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GMMScorerResults;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.ReadPairInfoToGMMResultsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/06/12
 * Time: 2:53 PM
 */
@Parameters(separators = "=", commandDescription = "Compute GMM features in each bin across the genome")
public class CommandGMMFitInsertSizes extends BaseCloudbreakCommand {

    @Parameter(names = {"--inputFileDescriptor"}, required = true, description = "HDFS path to the directory that holds the alignment records")
    String inputFileDescriptor;

    @Parameter(names = {"--outputHDFSDir"}, required = true, description = "HDFS path to the directory that will hold the output of the GMM procedure")
    String outputHDFSDir;

    @Parameter(names = {"--maxInsertSize"}, description = "Maximum insert size to consider (= max size of deletion detectable)")
    int maxInsertSize = Cloudbreak.DEFAULT_MAX_INSERT_SIZE;

    @Parameter(names = {"--faidx"}, required=true, description = "HDFS path to the chromosome length file for the reference genome")
    String faidxFileName;

    // todo pass in a single filter in the format chr:start-end
    @Parameter(names = {"--chrFilter"}, description = "If filter params are used, only consider alignments in the region chrFilter:startFilter-endFilter")
    String chrFilter;

    @Parameter(names = {"--startFilter"}, description = "See chrFilter")
    Long startFilter;

    @Parameter(names = {"--endFilter"}, description = "See chrFilter")
    Long endFilter;

    @Parameter(names = {"--excludePairsMappingIn"}, description = "HDFS path to a BED file. Any reads mapped within those intervals will be excluded from the processing")
    String exclusionRegionsFileName;

    @Parameter(names = {"--resolution"}, description = "Size of the bins to tile the genome with")
    int resolution = Cloudbreak.DEFAULT_RESOLUTION;

    @Parameter(names = {"--mapabilityWeighting"}, description = "HDFS path to a BigWig file containing genome uniqness scores. If specified, Cloudbreak will weight reads by the uniqueness of the regions they mapped to")
    String mapabilityWeightingFileName;

    // todo: validate aligner choice before launching job
    @Parameter(names = {"--aligner"}, description="Format of the alignment records (" + Cloudbreak.ALIGNER_GENERIC_SAM + "|" + Cloudbreak.ALIGNER_MRFAST + "|" + Cloudbreak.ALIGNER_NOVOALIGN + ")")
    String aligner = Cloudbreak.ALIGNER_GENERIC_SAM;

    @Parameter(names = {"--maxLogMapqDiff"}, description = "Adaptive quality score cutoff")
    Double maxLogMapqDiff = 5.0;

    @Parameter(names = {"--minScore"}, description = "Minimum alignment score (SAM tag AS); all reads with lower AS will be ignored")
    int minScore = -1;

    @Parameter(names = {"--maxMismatches"}, description = "Max number of mismatches allowed in an alignment; all other will be ignored")
    int maxMismatches = -1;

    @Parameter(names = {"--minCleanCoverage"}, description = "Minimum number of spanning read pairs for a bin to run the GMM fitting procedure")
    int minCleanCoverage = 3;

    // todo: remove?
    @Parameter(names = {"--legacyAlignments"}, description = "Use data generated with an older version of Cloudbreak")
    boolean legacyAlignments = false;

    @Parameter(names = {"--stripChromosomeNamesAtWhitespace"}, description = "Clip chromosome names from the reference at the first whitespace so they match with alignment fields")
    boolean stripChromosomeNamesAtWhitespace = false;

    public void run(Configuration conf) throws IOException, URISyntaxException {
        runHadoopJob(conf);
    }

    private void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("GMM Fit Insert Sizes");
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

        addDistributedCacheFile(conf, inputFileDescriptor, "read.group.info.file");
        addDistributedCacheFile(conf, faidxFileName, "alignment.faidx");

        if (exclusionRegionsFileName != null) {
            addDistributedCacheFile(conf, exclusionRegionsFileName, "alignment.exclusionRegions");
        }

        if (mapabilityWeightingFileName != null) {
            addDistributedCacheFile(conf, mapabilityWeightingFileName, "alignment.mapabilityWeighting");
        }

        DistributedCache.createSymlink(conf);

        conf.set("cloudbreak.resolution", String.valueOf(resolution));
        conf.set("cloudbreak.aligner", aligner);

        conf.set("pileupDeletionScore.maxInsertSize", String.valueOf(maxInsertSize));

        conf.set("max.log.mapq.diff", String.valueOf(maxLogMapqDiff));

        conf.set("pileupDeletionScore.minScore", String.valueOf(minScore));

        conf.set("pileupDeletionScore.maxMismatches", String.valueOf(maxMismatches));

        conf.set("min.clean.coverage", String.valueOf(minCleanCoverage));

        conf.set("legacy.alignments", String.valueOf(legacyAlignments));

        if (chrFilter != null) {
            conf.set("alignments.filterchr", chrFilter);
            conf.set("alignments.filterstart", startFilter.toString());
            conf.set("alignments.filterend", endFilter.toString());
        }
        conf.set("alignments.strip.chromosome.name.at.whitespace", String.valueOf(stripChromosomeNamesAtWhitespace));

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setMapperClass(AlignmentsToReadPairInfoMapper.class);
        conf.setMapOutputKeyClass(GenomicLocationWithQuality.class);
        conf.setMapOutputValueClass(ReadPairInfo.class);
        conf.setOutputKeyComparatorClass(GenomicLocationWithQualitySortComparator.class);
        conf.setOutputValueGroupingComparator(GenomicLocationWithQualityGroupingComparator.class);
        conf.setPartitionerClass(GenomicLocationWithQualityPartitioner.class);

        conf.setReducerClass(ReadPairInfoToGMMResultsReducer.class);

        conf.setOutputKeyClass(GenomicLocation.class);
        conf.setOutputValueClass(GMMScorerResults.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setCompressMapOutput(true);

        JobClient.runJob(conf);

    }
}
