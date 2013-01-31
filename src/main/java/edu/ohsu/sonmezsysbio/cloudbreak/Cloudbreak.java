package edu.ohsu.sonmezsysbio.cloudbreak;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import edu.ohsu.sonmezsysbio.cloudbreak.command.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The main class for Cloudbreak. Run "hadoop jar" on the assembled jar file to get the list of commands and options.
 */
public class Cloudbreak extends Configured implements Tool
{

    // Separators for the textual representation of alignments
    public static final String ALIGNMENT_SEPARATOR = "\tCB_ALIGN\t";
    public static final String READ_SEPARATOR = "\tCB_READ\t";

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
            command.run(getConf());

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

        CommandReadPairedEndFilesIntoHDFS readFiles = new CommandReadPairedEndFilesIntoHDFS();
        jc.addCommand("readPairedEndFilesIntoHDFS", readFiles);

        CommandReadSAMFileIntoHDFS readSamFile = new CommandReadSAMFileIntoHDFS();
        jc.addCommand("readSAMFileIntoHDFS", readSamFile);

        CommandNovoalignSingleEnds singleEnds  = new CommandNovoalignSingleEnds();
        jc.addCommand("alignSingleEnds", singleEnds);

        CommandBowtie2SingleEnds bowtie2SingleEnds  = new CommandBowtie2SingleEnds();
        jc.addCommand("bowtie2SingleEnds", bowtie2SingleEnds);

        CommandRazerS3SingleEnds razerS3SingleEnds  = new CommandRazerS3SingleEnds();
        jc.addCommand("razerS3SingleEnds", razerS3SingleEnds);

        CommandMrFastSingleEnds mrFastSingleEnds  = new CommandMrFastSingleEnds();
        jc.addCommand("mrfastSingleEnds", mrFastSingleEnds);

        CommandGMMFitSingleEndInsertSizes GMMFitSingleEndInsertSizes = new CommandGMMFitSingleEndInsertSizes();
        jc.addCommand("GMMFitSingleEndInsertSizes", GMMFitSingleEndInsertSizes);

        CommandExportWigAndBedFiles exportWigAndBedFiles = new CommandExportWigAndBedFiles();
        jc.addCommand("exportWigAndBedFiles", exportWigAndBedFiles);

        CommandExportGMMResults exportGMMResults = new CommandExportGMMResults();
        jc.addCommand("exportGMMResults", exportGMMResults);

        CommandExtractDeletionCalls commandExtractDeletionCalls = new CommandExtractDeletionCalls();
        jc.addCommand("extractDeletionCalls", commandExtractDeletionCalls);

        CommandExtractInsertionCalls commandExtractInsertionCalls = new CommandExtractInsertionCalls();
        jc.addCommand("extractInsertionCalls", commandExtractInsertionCalls);

        CommandDumpReadsWithScores dumpReadsWithScores = new CommandDumpReadsWithScores();
        jc.addCommand("dumpReadsWithScores", dumpReadsWithScores);

        CommandExtractPositiveRegionsFromWig commandExtractPositiveRegionsFromWig = new CommandExtractPositiveRegionsFromWig();
        jc.addCommand("extractPositiveRegionsFromWig", commandExtractPositiveRegionsFromWig);

        CommandDebugReadPairInfo commandDebugReadPairInfo = new CommandDebugReadPairInfo();
        jc.addCommand("debugReadPairInfo", commandDebugReadPairInfo);

        CommandFindAlignment commandFindAlignment = new CommandFindAlignment();
        jc.addCommand("findAlignment", commandFindAlignment);

        CommandSummarizeAlignments commandSummarizeAlignments = new CommandSummarizeAlignments();
        jc.addCommand("summarizeAlignments", commandSummarizeAlignments);

        CommandFindGenomicLocationsOverThreshold commandFindGenomicLocationsOverThreshold = new CommandFindGenomicLocationsOverThreshold();
        jc.addCommand("findGenomicLocationsOverThreshold", commandFindGenomicLocationsOverThreshold);

        CommandExportAlignmentsFromHDFS commandExportAlignmentsFromHDFS = new CommandExportAlignmentsFromHDFS();
        jc.addCommand("exportAlignmentsFromHDFS", commandExportAlignmentsFromHDFS);

        CommandSortGMMResults commandSortGMMResults = new CommandSortGMMResults();
        jc.addCommand("sortGMMResults", commandSortGMMResults);

        jc.setProgramName("Cloudbreak");
        return jc;
    }
}
