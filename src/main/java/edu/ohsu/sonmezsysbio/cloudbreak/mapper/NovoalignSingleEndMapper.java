package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;
import edu.ohsu.sonmezsysbio.cloudbreak.io.NovoalignAlignmentReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/21/11
 * Time: 5:36 PM
 */
public class NovoalignSingleEndMapper extends SingleEndAlignmentMapper {

    private static Logger logger = Logger.getLogger(NovoalignSingleEndMapper.class);

    private OutputCollector<Text, Text> output;
    private String localDir;
    private String reference;
    private String threshold;
    private String baseQualityFormat;
    private String novoalignExecutable;

    @Override
    public void configure(JobConf job) {
        super.configure(job);

        logger.info("Current dir: " + new File(".").getAbsolutePath());

        this.localDir = job.get("mapred.child.tmp");
        reference = job.get("novoalign.reference");
        threshold = job.get("novoalign.threshold");
        baseQualityFormat = job.get("novoalign.quality.format");
        novoalignExecutable = job.get("novoalign.executable");

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

        File indexFile = new File(reference);
        if (! indexFile.exists()) {
            logger.error("index file does not exist: " + indexFile.getPath());
        } else {
            logger.info("index file length: " + indexFile.length());
        }

        String[] commandLine = buildCommandLine(novoalignExecutable, reference, s1File.getPath(), threshold, baseQualityFormat);
        logger.info("Executing command: " + Arrays.toString(commandLine));
        Process p = Runtime.getRuntime().exec(commandLine);
        logger.debug("Exec'd");

        BufferedReader stdInput = new BufferedReader(new
                         InputStreamReader(p.getInputStream()));

        readAlignments(stdInput, p.getErrorStream());
    }

    protected void readAlignments(BufferedReader stdInput, InputStream errorStream) throws IOException {
        String outLine;
        NovoalignAlignmentReader alignmentReader = new NovoalignAlignmentReader();
        while ((outLine = stdInput.readLine()) != null) {
            if (outLine.startsWith("#"))  {
                logger.info("COMMENT LINE: " + outLine);
                continue;
            }
            if (outLine.startsWith("Novoalign") || outLine.startsWith("Exception")) {
                String error = printErrorStream(errorStream);
                throw new RuntimeException(error);
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

    protected static String[] buildCommandLine(String novoalignExecutable, String reference, String path1, String threshold, String baseQualityFormat) {
        String[] commandArray = {
                "./" + novoalignExecutable,
                "-d", reference,
                "-c", "1",
                "-f", path1,
                "-F", baseQualityFormat,
                "-k", "-K", "calfile.txt", "-q", "5",
                "-r", "Ex", "100", "-t", threshold, "-x", "10"
        };
        return commandArray;
    }
}
