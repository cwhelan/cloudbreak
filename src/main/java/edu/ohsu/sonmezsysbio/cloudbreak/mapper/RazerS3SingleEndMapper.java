package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.io.SAMAlignmentReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/21/11
 * Time: 5:36 PM
 */
public class RazerS3SingleEndMapper extends SingleEndAlignerMapper {

    private static Logger logger = Logger.getLogger(RazerS3SingleEndMapper.class);

    //{ logger.setLevel(Level.DEBUG); }

    private OutputCollector<Text, Text> output;
    private String localDir;
    private String reference;
    private String numReports;
    private String razerS3Executable;
    private String pctIdentity;
    private String sensitivity;

    @Override
    protected boolean getCompressTempReadFile() {
        return false;
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);

        logger.debug("Current dir: " + new File(".").getAbsolutePath());

        this.localDir = job.get("mapred.child.tmp");
        reference = job.get("razers3.reference");
        numReports = job.get("razers3.num.reports");
        pctIdentity = job.get("razers3.pct.identity");
        sensitivity = job.get("razers3.sensitivity");
        razerS3Executable = job.get("razers3.executable");

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
        String[] commandLine = buildCommandLine(razerS3Executable, referenceBaseName, s1File.getPath(), numReports, pctIdentity, sensitivity);
        logger.debug("Executing command: " + Arrays.toString(commandLine));
        ReportableProcess p = new ReportableProcess(Runtime.getRuntime().exec(commandLine), reporter);
        logger.debug("Exec'd");

        try {
            p.waitForWhileReporting();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        logger.debug("done");

        BufferedReader stdInput = new BufferedReader(new FileReader("map.result"));
        readAlignments(stdInput, p.process.getErrorStream());
    }


    protected static String[] buildCommandLine(String razers3Executable, String referenceBaseName,
                                               String path1, String numReports, String pctIdentity,
                                               String sensitivity
    ) {
        String[] commandArray = {
                "./" + razers3Executable,
                "-o", "map.result", "-of", "sam", "-rr", sensitivity, "-i", pctIdentity, "-m", numReports,
                "--full-readid",
                referenceBaseName,
                path1
        };
        return commandArray;
    }

    @Override
    protected String getCommandName() {
        return "razers3";
    }
}
