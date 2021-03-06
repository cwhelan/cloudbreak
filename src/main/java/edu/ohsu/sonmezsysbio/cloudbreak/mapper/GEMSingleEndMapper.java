package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import com.google.common.base.Joiner;
import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.io.SAMAlignmentReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
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
public class GEMSingleEndMapper extends SingleEndAlignerMapper {
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

    @Override
    protected String getCommandName() {
        return "gem-mapper";
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

}
