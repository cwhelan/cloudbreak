package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import com.google.common.base.Joiner;
import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.io.SAMAlignmentReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/21/11
 * Time: 5:36 PM
 */
public class BWAPairedEndMapper extends PairedEndAlignerMapper {

    private static Logger logger = Logger.getLogger(BWAPairedEndMapper.class);

    //{ logger.setLevel(Level.DEBUG); }

    private OutputCollector<Text, Text> output;
    private String localDir;
    private String referenceBase;
    private String numExtraReports;
    private String bwaExecutable;
    private String xa2multiExecutable;
    private int maxProcessesOnNode;

    @Override
    protected boolean getCompressTempReadFile() {
        return false;
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);

        logger.debug("Current dir: " + new File(".").getAbsolutePath());

        this.localDir = job.get("mapred.child.tmp");
        referenceBase = job.get("bwa.reference");
        numExtraReports = job.get("bwa.num.extra.reports");
        bwaExecutable = job.get("bwa.executable");
        xa2multiExecutable = job.get("bwa.xa2multi");
        maxProcessesOnNode = Integer.parseInt(job.get("bwa.max.processes.on.node"));
    }

    @Override
    public void close() throws IOException {
        super.close();

        if (! s1File.exists()) {
            logger.error("file does not exist: " + s1File.getPath());
        } else {
            logger.info("read file length: " + s1File.length());
        }

        try {
            waitForOpenSlot(maxProcessesOnNode, reporter);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        String referenceBaseName = new File(referenceBase).getName();
        String[] commandLine = buildAlnCommandLine(bwaExecutable, referenceBaseName, s1File.getPath(), numExtraReports, xa2multiExecutable);
        logger.info("Executing command: " + Arrays.toString(commandLine));
        Process aln1 = Runtime.getRuntime().exec(commandLine);
        logger.debug("Exec'd");
        try {
            aln1.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(e);
            throw new RuntimeException(e);
        }

        commandLine = buildAlnCommandLine(bwaExecutable, referenceBaseName, s2File.getPath(), numExtraReports, xa2multiExecutable);
        logger.info("Executing command: " + Arrays.toString(commandLine));
        Process aln2 = Runtime.getRuntime().exec(commandLine);
        logger.debug("Exec'd");
        try {
            aln2.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(e);
            throw new RuntimeException(e);
        }

        commandLine = buildSampeCommandLine(bwaExecutable, referenceBaseName, s1File.getPath(), s2File.getPath(), numExtraReports, xa2multiExecutable);
        logger.info("Executing command: " + Arrays.toString(commandLine));
        Process sampe = Runtime.getRuntime().exec(commandLine);
        logger.debug("Exec'd");

        BufferedReader stdInput = new BufferedReader(new
                         InputStreamReader(sampe.getInputStream()));

        readAlignments(stdInput, sampe.getErrorStream());
    }

    protected void readAlignments(BufferedReader stdInput, InputStream errorStream) throws IOException {
        String outLine;
        SAMAlignmentReader alignmentReader = new SAMAlignmentReader();
        while ((outLine = stdInput.readLine()) != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("LINE: " + outLine);
            }
            if (outLine.startsWith("@"))  {
                logger.debug("SAM HEADER LINE: " + outLine);
                continue;
            }

            String readPairId = outLine.substring(0,outLine.indexOf('\t')-2);
            AlignmentRecord alignment = alignmentReader.parseRecord(outLine);

            if (! alignment.isMapped()) {
                continue;
            }

            getOutput().collect(new Text(readPairId), new Text(outLine));

        }

        String errLine;
        BufferedReader errorReader = new BufferedReader(new InputStreamReader(errorStream));
        while ((errLine = errorReader.readLine()) != null) {
            logger.error("ERROR: " + errLine);
        }
    }

    private String printErrorStream(InputStream errorStream) throws IOException {
        String outLine;BufferedReader stdErr = new BufferedReader(new
                InputStreamReader(errorStream));
        String firstErrorLine = null;
        while ((outLine = stdErr.readLine()) != null) {
            if (firstErrorLine == null) firstErrorLine = outLine;
            logger.error(outLine);
        }
        return firstErrorLine;
    }

    protected static String[] buildAlnCommandLine(String bwaExecutable, String referenceBaseName, String path, String numReports, String xa2multiExecutable) {
        String alnOptions = "-e 5";
        String[] commandArray = {
                "/bin/bash", "-c", Joiner.on(" ").join(new String[] {"./bwa", "aln", alnOptions, referenceBaseName, path, ">", path + ".sai"})
        };
        return commandArray;
    }

    protected static String[] buildSampeCommandLine(String bwaExecutable, String referenceBaseName, String path1, String path2, String numReports, String xa2multiExecutable) {
        String[] commandArray = {
                "./bwa", "sampe", referenceBaseName, path1 + ".sai", path2 + ".sai", path1, path2
        };
        return commandArray;
    }

    @Override
    protected String getCommandName() {
        return "bwa";
    }
}
