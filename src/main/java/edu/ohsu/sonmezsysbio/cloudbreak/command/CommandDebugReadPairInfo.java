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
import edu.ohsu.sonmezsysbio.cloudbreak.partitioner.GenomicLocationWithQualityPartitioner;
import edu.ohsu.sonmezsysbio.svpipeline.io.GenomicLocation;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.SingleEndAlignmentsToReadPairInfoMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/9/12
 * Time: 11:12 AM
 */
@Parameters(separators = "=", commandDescription = "View read pair infos")
public class CommandDebugReadPairInfo extends BaseCloudbreakCommand {

    @Parameter(names = {"--inputFileDescriptor"}, required = true)
    String inputFileDescriptor;

    @Parameter(names = {"--outputHDFSDir"}, required = true)
    String ouptutHDFSDir;

    @Parameter(names = {"--maxInsertSize"})
    int maxInsertSize = 500000;

    @Parameter(names = {"--faidx"}, required=true)
    String faidxFileName;

    @Parameter(names = {"--chrFilter"}, required = true)
    String chrFilter;

    @Parameter(names = {"--startFilter"}, required = true)
    Long startFilter;

    @Parameter(names = {"--endFilter"}, required = true)
    Long endFilter;

    @Parameter(names = {"--resolution"})
    int resolution = Cloudbreak.DEFAULT_RESOLUTION;

    @Parameter(names = {"--excludePairsMappingIn"})
    String exclusionRegionsFileName;

    @Parameter(names = {"--mapabilityWeighting"})
    String mapabilityWeightingFileName;

    @Parameter(names = {"--aligner"})
    String aligner = Cloudbreak.ALIGNER_NOVOALIGN;

    @Parameter(names = {"--minScore"})
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

        conf.setMapperClass(SingleEndAlignmentsToReadPairInfoMapper.class);
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
