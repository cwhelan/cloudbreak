package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.SAMRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.io.SAMAlignmentReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/21/11
 * Time: 5:36 PM
 */
public class Bowtie2PairedEndMapper extends PairedEndAlignmentMapper {

    private static Logger logger = Logger.getLogger(Bowtie2PairedEndMapper.class);

    //{ logger.setLevel(Level.DEBUG); }

    private OutputCollector<Text, Text> output;
    private String localDir;
    private String reference;
    private String numReports;
    private String bowtie2Executable;
    private int targetIsize;
    private int targetIsizeSD;
    private int isizeRange;

    @Override
    protected boolean getCompressTempReadFile() {
        return false;
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);

        logger.debug("Current dir: " + new File(".").getAbsolutePath());

        this.localDir = job.get("mapred.child.tmp");
        reference = job.get("bowtie2.reference");
        numReports = job.get("bowtie2.num.reports");
        bowtie2Executable = job.get("bowtie2.executable");
        targetIsize = Integer.parseInt(job.get("bowtie2.targetIsize"));
        targetIsizeSD = Integer.parseInt(job.get("bowtie2.targetIsizeSD"));
        isizeRange = Integer.parseInt(job.get("bowtie2.isizeRange"));
    }

    @Override
    public void close() throws IOException {
        super.close();

        s1FileWriter.close();
        s2FileWriter.close();

        if (! s1File.exists()) {
            logger.error("file does not exist: " + s1File.getPath());
        } else {
            logger.info("read file length: " + s1File.length());
        }

        if (! s2File.exists()) {
            logger.error("file does not exist: " + s2File.getPath());
        } else {
            logger.info("read file length: " + s2File.length());
        }

        String referenceBaseName = new File(reference).getName();
        int minConcIsize = targetIsize - isizeRange * targetIsizeSD;
        int maxConcIsize = targetIsize + isizeRange * targetIsizeSD;
        String[] commandLine = buildCommandLine(bowtie2Executable, referenceBaseName,
                s1File.getPath(), s2File.getPath(), numReports,
                minConcIsize, maxConcIsize);
        logger.debug("Executing command: " + Arrays.toString(commandLine));
        Process p = Runtime.getRuntime().exec(commandLine);
        logger.debug("Exec'd");

        BufferedReader stdInput = new BufferedReader(new
                         InputStreamReader(p.getInputStream()));

        readAlignments(stdInput, p.getErrorStream());
    }

    protected void readAlignments(BufferedReader stdInput, InputStream errorStream) throws IOException {
        String outLine;
        SAMAlignmentReader alignmentReader = new SAMAlignmentReader();
        String currentReadPairId = null;
        Set<String> r1Locations = new HashSet<String>();
        Set<String> r2Locations = new HashSet<String>();
        while ((outLine = stdInput.readLine()) != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("LINE: " + outLine);
            }
            if (outLine.startsWith("@"))  {
                logger.debug("SAM HEADER LINE: " + outLine);
                continue;
            }

            String readPairId = outLine.substring(0,outLine.indexOf('\t'));
            if (!readPairId.equals(currentReadPairId)) {
                r1Locations.clear();
                r2Locations.clear();
                currentReadPairId = readPairId;
            }

            SAMRecord alignment = (SAMRecord) alignmentReader.parseRecord(outLine);
            if (! alignment.isMapped()) {
                continue;
            }

            String location = alignment.getChromosomeName() + ":" + alignment.getPosition();
            if (alignment.isAlignmentOfFirstRead()) {
                if (r1Locations.contains(location)) {
                    continue;
                } else {
                    r1Locations.add(location);
                }
            } else {
                if (r2Locations.contains(location)) {
                    continue;
                } else {
                    r2Locations.add(location);
                }
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

    protected static String[] buildCommandLine(String bowtie2executable, String referenceBaseName,
                                               String path1, String path2, String numReports,
                                               int minConcIsize, int maxConcIsize) {
        // todo adjust insert size here
        String[] commandArray = {
                "./" + bowtie2executable,
                "-x", referenceBaseName,
                "-1", path1,
                "-2", path2,
                "-k", numReports,
                "--very-sensitive-local", "--mm", "--score-min", "L,0,1.75", "--no-unal",
                "-I", String.valueOf(minConcIsize), "-X", String.valueOf(maxConcIsize)
        };
        return commandArray;
    }
}
