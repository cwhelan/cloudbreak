package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

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
public class Bowtie2SingleEndMapper extends SingleEndAlignerMapper {

    private static org.apache.log4j.Logger logger = Logger.getLogger(Bowtie2SingleEndMapper.class);

    //{ logger.setLevel(Level.DEBUG); }

    private OutputCollector<Text, Text> output;
    private String localDir;
    private String reference;
    private String numReports;
    private String bowtie2Executable;

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

    }

    @Override
    public void close() throws IOException {
        super.close();

        if (! s1File.exists()) {
            logger.error("file does not exist: " + s1File.getPath());
        } else {
            logger.info("read file length: " + s1File.length());
        }

        String referenceBaseName = new File(reference).getName();
        String[] commandLine = buildCommandLine(bowtie2Executable, referenceBaseName, s1File.getPath(), numReports);
        logger.debug("Executing command: " + Arrays.toString(commandLine));
        Process p = Runtime.getRuntime().exec(commandLine);
        logger.debug("Exec'd");

        BufferedReader stdInput = new BufferedReader(new
                         InputStreamReader(p.getInputStream()));

        readAlignments(stdInput, p.getErrorStream());
    }


    protected static String[] buildCommandLine(String bowtie2executable, String referenceBaseName, String path1, String numReports) {
        String[] commandArray = {
                "./" + bowtie2executable,
                "-x", referenceBaseName,
                "-U", path1,
                "-k", numReports,
                "--very-sensitive-local", "--mm", "--score-min", "L,0,1"
        };
        return commandArray;
    }

    @Override
    protected String getCommandName() {
        return "bowtie2";
    }
}
