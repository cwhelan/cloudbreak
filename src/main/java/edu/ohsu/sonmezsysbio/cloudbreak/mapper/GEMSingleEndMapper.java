package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import com.google.common.base.Joiner;
import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.io.SAMAlignmentReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 1/23/13
 * Time: 10:08 AM
 */
public class GEMSingleEndMapper extends SingleEndAlignmentMapper {
    private static Logger logger = Logger.getLogger(GEMSingleEndMapper.class);

    { logger.setLevel(Level.INFO); }

    private OutputCollector<Text, Text> output;
    private String localDir;
    private String reference;
    private String gemMapperExecutable;
    private String gem2SamExecutable;

    private String numReports;
    private String editDistance;
    private String strata;
    private int maxGemProcessesOnNode;

    @Override
    protected boolean getCompressTempReadFile() {
        return false;
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);

        logger.debug("Current dir: " + new File(".").getAbsolutePath());

        this.localDir = job.get("mapred.child.tmp");
        reference = job.get("gem.reference");
        numReports = job.get("gem.num.reports");
        editDistance = job.get("gem.edit.distance");
        strata = job.get("gem.strata");
        gemMapperExecutable = job.get("gem.mapper.executable");
        gem2SamExecutable = job.get("gem.tosam.executable");
        maxGemProcessesOnNode = Integer.parseInt(job.get("gem.max.processes.on.node"));
    }

    @Override
    public void close() throws IOException {
        super.close();

        s1FileWriter.close();

        if (! s1File.exists()) {
            logger.error("file does not exist: " + s1File.getPath());
        } else {
            logger.info("read file length: " + s1File.length());
        }

        String referenceBaseName = new File(reference).getName();
        String[] commandLine = buildCommandLine(gemMapperExecutable, gem2SamExecutable, referenceBaseName, s1File.getPath(), numReports, editDistance, strata);
        logger.info("Executing command: " + Arrays.toString(commandLine));

        try {
            waitForOpenSlot(maxGemProcessesOnNode, reporter);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        ReportableProcess p = new ReportableProcess(Runtime.getRuntime().exec(commandLine), reporter);
        logger.info("Exec'd");

        try {
            int exitVal = p.waitForWhileReporting();
            logger.info("process returned with exit code: " + exitVal);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        logger.info("done");

        File resultFile = new File("map.result");
        if (resultFile.exists()) {
            BufferedReader stdInput = new BufferedReader(new FileReader(resultFile));
            readAlignments(stdInput, p.process.getErrorStream());
        } else {
            printErrorStream(p.process.getErrorStream());
        }
    }

    private void waitForOpenSlot(int maxGemProcessesOnNode, Reporter reporter) throws IOException, InterruptedException {
        while (true) {
            // sleep for a random length of time between 0 and 60 seconds
            long sleepTime = (long) (Math.random() * 1000 * 60);
            logger.info("sleeping for " + sleepTime);
            Thread.sleep(sleepTime);
            int numRunningMappers = getNumRunningMappers();
            logger.info("num running mappers: " + numRunningMappers);
            if (numRunningMappers < maxGemProcessesOnNode) return;
            reporter.progress();
        }
    }

    private int getNumRunningMappers() throws IOException {
        int numRunningMappers = 0;
        Runtime runtime = Runtime.getRuntime();
        String cmds[] = {"ps", "-C", getCommandName(), "--no-headers"};
        Process proc = runtime.exec(cmds);
        InputStream inputstream = proc.getInputStream();
        InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
        BufferedReader bufferedreader = new BufferedReader(inputstreamreader);
        while (bufferedreader.readLine() != null) {
            numRunningMappers++;
        }

        return numRunningMappers;
    }

    private String getCommandName() {
        return "gem-mapper";
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

    protected static String[] buildCommandLine(String gemMapperExecutable, String gem2SamExecutable, String referenceBaseName,
                                               String path1, String numReports, String editDistance, String strata
    ) {
        String[] commandArray = {
                "bash", "-c", Joiner.on(" ").join(new String[] {"./" + gemMapperExecutable,
                "-I", referenceBaseName, "-i", path1, "-q", "ignore", "-m", editDistance, "-e", editDistance, "-d", numReports, "-s", strata, "--max-big-indel-length",
                "0", "|", "./" + gem2SamExecutable, "-o", "map.result", "--expect-single-end-reads"})
        };
        return commandArray;
    }

    private class ReportableProcess {
        private Process process;
        private Reporter reporter;

        public ReportableProcess(Process exec, Reporter reporter) {
            this.process = exec;
            this.reporter = reporter;
        }

        public int waitForWhileReporting() throws InterruptedException {
            while (true) {
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    throw e;
                }
                try {
                    int exitVal = process.exitValue();
                    return exitVal;
                } catch (IllegalThreadStateException e) {
                    reporter.progress();
                }
            }
        }
    }

}
