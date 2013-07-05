package edu.ohsu.sonmezsysbio.cloudbreak;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import edu.ohsu.sonmezsysbio.cloudbreak.command.*;
import edu.ohsu.sonmezsysbio.cloudbreak.io.HDFSWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

/**
 * The main class for Cloudbreak. Run "hadoop jar" on the assembled jar file to get the list of commands and options.
 */
public class Cloudbreak extends Configured implements Tool
{

    // Separators for the textual representation of alignments
    public static final String ALIGNMENT_SEPARATOR = "\tCB_ALIGN\t";
    public static final String READ_SEPARATOR = "\tCB_READ\t";

    public static final String LEGACY_ALIGNMENT_SEPARATOR = "\tSVP_ALIGNMENT\t";
    public static final String LEGACY_READ_SEPARATOR = "\tSVP_READ\t";

    // default values used in multiple commands
    public static final int DEFAULT_RESOLUTION = 25;
    public static final int WINDOW_SIZE_IN_LINES = 1000;
    public static final int DEFAULT_MAX_INSERT_SIZE = 25000;

    // constants identifying alignement formats recognized by Cloudbreak
    public static final String ALIGNER_NOVOALIGN = "novoalign";
    public static final String ALIGNER_MRFAST = "mrfast";
    public static final String ALIGNER_GENERIC_SAM = "sam";

    // types of variants identified by cloudbreak
    public static final String VARIANT_TYPE_INSERTION = "INS";
    public static final String VARIANT_TYPE_DELETION = "DEL";
    public static final String VARIANT_TYPE_UNKNOWN = "NA";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Cloudbreak(), args);
        System.exit(res);
    }

    public static HDFSWriter getHdfsWriter(Configuration config, FileSystem hdfs, Path p, String compress) throws IOException {
        HDFSWriter writer = new HDFSWriter();
        if ("snappy".equals(compress)) {
            writer.seqFileWriter = SequenceFile.createWriter(hdfs, config, p, Text.class, Text.class, SequenceFile.CompressionType.BLOCK, new SnappyCodec());
        } else {
            FSDataOutputStream outputStream = hdfs.create(p);
            BufferedWriter bufferedWriter = null;
            if ("gzip".equals(compress)) {
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(outputStream)));
            } else {
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
            }
            writer.textFileWriter = bufferedWriter;
        }
        return writer;
    }

    public int run(String[] args) throws Exception {
        JCommander jc = buildJCommander();

        String parsedCommand = null;

        try {
            jc.parse(args);

            parsedCommand = jc.getParsedCommand();

            if (parsedCommand == null) {
                jc.usage();
                return 1;
            }
            CloudbreakCommand command = (CloudbreakCommand) jc.getCommands().get(parsedCommand).getObjects().get(0);
            try {
                command.run(getConf());
            } catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
                jc.usage(jc.getParsedCommand());
            }

            return 0;
        } catch (ParameterException pe) {
            System.err.println(pe.getMessage());
            return 1;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    protected static JCommander buildJCommander() {
        JCommander jc = new JCommander(new CommanderMain());

        // commands for importing data into HDFS
        CommandReadPairedEndFilesIntoHDFS readFiles = new CommandReadPairedEndFilesIntoHDFS();
        jc.addCommand("readPairedEndFilesIntoHDFS", readFiles);

        CommandReadSAMFileIntoHDFS readSamFile = new CommandReadSAMFileIntoHDFS();
        jc.addCommand("readSAMFileIntoHDFS", readSamFile);

//        CommandPrepSAMRecords commandPrepSAMRecords = new CommandPrepSAMRecords();
//        jc.addCommand("prepSAMRecords", commandPrepSAMRecords);

        // Alignment commands
        CommandBWAPairedEnds bwaPairedEnds  = new CommandBWAPairedEnds();
        jc.addCommand("bwaPairedEnds", bwaPairedEnds);

        CommandNovoalignSingleEnds singleEnds  = new CommandNovoalignSingleEnds();
        jc.addCommand("novoalignSingleEnds", singleEnds);

        CommandBowtie2SingleEnds bowtie2SingleEnds  = new CommandBowtie2SingleEnds();
        jc.addCommand("bowtie2SingleEnds", bowtie2SingleEnds);

        CommandGEMSingleEnds gemSingleEnds  = new CommandGEMSingleEnds();
        jc.addCommand("gemSingleEnds", gemSingleEnds);

        CommandRazerS3SingleEnds razerS3SingleEnds  = new CommandRazerS3SingleEnds();
        jc.addCommand("razerS3SingleEnds", razerS3SingleEnds);

        CommandMrFastSingleEnds mrFastSingleEnds  = new CommandMrFastSingleEnds();
        jc.addCommand("mrfastSingleEnds", mrFastSingleEnds);

        CommandExportAlignmentsFromHDFS commandExportAlignmentsFromHDFS = new CommandExportAlignmentsFromHDFS();
        jc.addCommand("exportAlignmentsFromHDFS", commandExportAlignmentsFromHDFS);

        // GMM fit command
        CommandGMMFitInsertSizes GMMFitSingleEndInsertSizes = new CommandGMMFitInsertSizes();
        jc.addCommand("GMMFitSingleEndInsertSizes", GMMFitSingleEndInsertSizes);

        // Variant extraction commands
        CommandExtractDeletionCalls commandExtractDeletionCalls = new CommandExtractDeletionCalls();
        jc.addCommand("extractDeletionCalls", commandExtractDeletionCalls);

        CommandExtractInsertionCalls commandExtractInsertionCalls = new CommandExtractInsertionCalls();
        jc.addCommand("extractInsertionCalls", commandExtractInsertionCalls);

        // commands to help with running on a cloud provider
        CommandCopyToS3 commandCopyFileToS3 = new CommandCopyToS3();
        jc.addCommand("copyToS3", commandCopyFileToS3);

        CommandLaunchCluster commandLaunchCluster = new CommandLaunchCluster();
        jc.addCommand("launchCluster", commandLaunchCluster);

        CommandRunScriptOnCluster commandRunScriptOnCluster = new CommandRunScriptOnCluster();
        jc.addCommand("runScriptOnCluster", commandRunScriptOnCluster);

        CommandDestroyCluster commandDestroyCluster = new CommandDestroyCluster();
        jc.addCommand("destroyCluster", commandDestroyCluster);

        // commands that let you export extra information from HDFS, for debugging or extra analysis
        CommandSummarizeAlignments commandSummarizeAlignments = new CommandSummarizeAlignments();
        jc.addCommand("summarizeAlignments", commandSummarizeAlignments);

        CommandExportGMMResults exportGMMResults = new CommandExportGMMResults();
        jc.addCommand("exportGMMResults", exportGMMResults);

        CommandDumpReadsWithScores dumpReadsWithScores = new CommandDumpReadsWithScores();
        jc.addCommand("dumpReadsWithScores", dumpReadsWithScores);

        CommandDebugReadPairInfo commandDebugReadPairInfo = new CommandDebugReadPairInfo();
        jc.addCommand("debugReadPairInfo", commandDebugReadPairInfo);

        CommandFindAlignment commandFindAlignment = new CommandFindAlignment();
        jc.addCommand("findAlignment", commandFindAlignment);

        // utilties (unsupported)
        CommandSortGMMResults commandSortGMMResults = new CommandSortGMMResults();
        jc.addCommand("sortGMMResults", commandSortGMMResults);

        CommandSplitSequenceFile commandSplitSequenceFile = new CommandSplitSequenceFile();
        jc.addCommand("splitSequenceFile", commandSplitSequenceFile);

//        CommandFindGenomicLocationsOverThreshold commandFindGenomicLocationsOverThreshold = new CommandFindGenomicLocationsOverThreshold();
//        jc.addCommand("findGenomicLocationsOverThreshold", commandFindGenomicLocationsOverThreshold);
//
//        CommandExportWigAndBedFiles exportWigAndBedFiles = new CommandExportWigAndBedFiles();
//        jc.addCommand("exportWigAndBedFiles", exportWigAndBedFiles);
//
//        CommandExtractPositiveRegionsFromWig commandExtractPositiveRegionsFromWig = new CommandExtractPositiveRegionsFromWig();
//        jc.addCommand("extractPositiveRegionsFromWig", commandExtractPositiveRegionsFromWig);

        jc.setProgramName("Cloudbreak");
        return jc;
    }
}
