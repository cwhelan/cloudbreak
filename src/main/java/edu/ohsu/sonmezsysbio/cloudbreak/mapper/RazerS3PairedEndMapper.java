package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

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
public class RazerS3PairedEndMapper extends PairedEndAlignmentMapper {

    private static Logger logger = Logger.getLogger(RazerS3PairedEndMapper.class);

    //{ logger.setLevel(Level.DEBUG); }

    private OutputCollector<Text, Text> output;
    private String localDir;
    private String reference;
    private String numReports;
    private String razersExecutable;
    private int maxIsize;

    @Override
    protected boolean getCompressTempReadFile() {
        return false;
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);

        logger.debug("Current dir: " + new File(".").getAbsolutePath());

        this.localDir = job.get("mapred.child.tmp");
        reference = job.get("razers.reference");
        numReports = job.get("razers.num.reports");
        razersExecutable = job.get("razers.executable");
        maxIsize = Integer.parseInt(job.get("razers.maxIsize"));
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
        int midIsize = maxIsize / 2;
        int isizeError = maxIsize / 2;
        String[] commandLine = buildCommandLine(razersExecutable, referenceBaseName,
                s1File.getPath(), s2File.getPath(), numReports,
                midIsize, isizeError);
        logger.debug("Executing command: " + Arrays.toString(commandLine));
        Process p = Runtime.getRuntime().exec(commandLine);
        logger.debug("Exec'd");
        try {
            p.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        logger.debug("done");

        BufferedReader stdInput = new BufferedReader(new
                FileReader("map.result"));

        readAlignments(stdInput, p.getErrorStream());
    }

    protected void readAlignments(BufferedReader input, InputStream errorStream) throws IOException {
        String outLine;
        SAMAlignmentReader alignmentReader = new SAMAlignmentReader();
        String currentReadPairId = null;
        Set<String> r1Locations = new HashSet<String>();
        Set<String> r2Locations = new HashSet<String>();
        while ((outLine = input.readLine()) != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("LINE: " + outLine);
            }
            if (outLine.startsWith("@"))  {
                logger.debug("SAM HEADER LINE: " + outLine);
                continue;
            }

            String readPairId = outLine.substring(0,outLine.indexOf('\t') - 2);
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

    protected static String[] buildCommandLine(String razers3Executable, String referenceBaseName,
                                               String path1, String path2, String numReports,
                                               int midIsize, int isizeError) {
        // todo adjust insert size here
        String[] commandArray = {
                "./" + razers3Executable,
                "-o", "map.result", "-of", "sam", "-rr", "100", "-i", "96", "-m", numReports,
                "--library-length", String.valueOf(midIsize), "--library-error", String.valueOf(isizeError),
                referenceBaseName,
                path1,
                path2
        };
        return commandArray;
    }
}
