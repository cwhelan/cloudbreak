package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 2/4/12
 * Time: 3:38 PM
 */
@Parameters(separators = "=", commandDescription = "Load paired fastq files into HDFS")
public class CommandReadPairedEndFilesIntoHDFS implements CloudbreakCommand {

    private static org.apache.log4j.Logger log = Logger.getLogger(CommandReadPairedEndFilesIntoHDFS.class);

    @Parameter(names = {"--HDFSDataDir"}, required = true)
    String hdfsDataDir;

    @Parameter(names = {"--fastqFile1"}, required = true)
    String readFile1;

    @Parameter(names = {"--fastqFile2"}, required = true)
    String readFile2;

    @Parameter(names = {"--outFileName"})
    String outFileName = "reads";

    @Parameter(names = {"--compress"})
    String compress = "snappy";

    @Parameter(names = {"--clipReadIdsAtWhitespace"})
    boolean clipReadIdsAtWhitespace = true;

    @Parameter(names = {"--trigramEntropyFilter"})
    Double trigramEntropyFilter = -1.0;

    private long numRecords;
    private long numFilteredRecords;

    public void copyReadFilesToHdfs() throws IOException {
        Configuration config = new Configuration();

        FileSystem hdfs = FileSystem.get(config);
        Path p = new Path(hdfsDataDir + "/" + outFileName);

        HDFSWriter writer = new HDFSWriter();
        if ("snappy".equals(compress)) {
            writer.seqFileWriter = SequenceFile.createWriter(hdfs, config, p, LongWritable.class, Text.class, SequenceFile.CompressionType.BLOCK, new SnappyCodec());
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
        try {
            readFile(writer, readFile1, readFile2);
        } finally {
            writer.close();
        }

    }

    private void readFile(HDFSWriter writer, String pathname1, String pathname2) throws IOException {
        BufferedReader inputReader1;
        BufferedReader inputReader2 = null;

        inputReader1 = openFile(pathname1);
        inputReader2 = openFile(pathname2);

        numRecords = 0;
        try {
            String convertedFastqLine = readFastqEntries(inputReader1, inputReader2, trigramEntropyFilter);
            while (convertedFastqLine != null) {
                writer.write(new LongWritable(numRecords), convertedFastqLine);
                convertedFastqLine = readFastqEntries(inputReader1, inputReader2, trigramEntropyFilter);
                numRecords++;
            }
        } finally {
            inputReader1.close();
        }
    }

    private BufferedReader openFile(String pathname) throws IOException {
        BufferedReader inputReader1;
        if (pathname.endsWith("gz")) {
            inputReader1 = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(pathname))));
        } else {
            inputReader1 = new BufferedReader(new FileReader(new File(pathname)));
        }
        return inputReader1;
    }

    private String readFastqEntries(BufferedReader inputReader1, BufferedReader inputReader2, Double trigramEntropyFilter) throws IOException {
        StringBuffer lineBuffer;
        while (true) {
            String read1 = inputReader1.readLine();
            if (read1 == null) {
                return null;
            }

            String seq1 = inputReader1.readLine();
            String sep1 = inputReader1.readLine();
            String qual1 = inputReader1.readLine();

            String read2 = inputReader2.readLine();
            if (read2 == null) {
                return null;
            }

            String seq2 = inputReader2.readLine();
            String sep2 = inputReader2.readLine();
            String qual2 = inputReader2.readLine();

            if (clipReadIdsAtWhitespace) {
                read1 = read1.split("\\s+")[0];
                read2 = read2.split("\\s+")[0];
            }

            String readPrefix = greatestCommonPrefix(read1, read2);

            if (! (passesEntropyFilter(seq1, trigramEntropyFilter) && passesEntropyFilter(seq2, trigramEntropyFilter))) {
                log.debug("Skipping read pair " + readPrefix + " because seqs " + seq1 + ", " + seq2 + " don't pass entropy filter.");
                numFilteredRecords++;
                continue;
            }

            lineBuffer = new StringBuffer();

            lineBuffer.append(readPrefix);
            lineBuffer.append("/1");
            lineBuffer.append("\t").append(seq1).append("\t").append(sep1).append("\t").append(qual1);
            lineBuffer.append("\n");

            lineBuffer.append(readPrefix);
            lineBuffer.append("/2");
            lineBuffer.append("\t").append(seq2).append("\t").append(sep2).append("\t").append(qual2);
            lineBuffer.append("\n");
            break;
        }

        return lineBuffer.toString();
    }

    private boolean passesEntropyFilter(String seq, Double trigramEntropyFilter) {
        if (trigramEntropyFilter < 0) {
            return true;
        } else {
            return trigramEntropy(seq) > trigramEntropyFilter;
        }
    }

    protected Double trigramEntropy(String seq) {
        Map<String, Double> counts = new HashMap<String, Double>();
        for (int i = 2; i < seq.length(); i++) {
            String trigram = seq.substring(i - 2, i);
            if (counts.containsKey(trigram)) {
                counts.put(trigram, counts.get(trigram) + 1);
            } else {
                counts.put(trigram, 1.0);
            }
        }
        Double entropy = 0.0;
        for (String trigram : counts.keySet()) {
            double normalizedCount = counts.get(trigram) / (seq.length() - 2);
            entropy += normalizedCount * Math.log(normalizedCount);
        }
        return -1 * entropy;
    }

    protected static String greatestCommonPrefix(String read1, String read2) {
        int i = 0;
        // find the greatest common prefix, ignoring the '/1' and '/2' at the end of older fastq reads
        while (i < Math.max(read1.length(), read2.length()) && read1.charAt(i) == read2.charAt(i) &&
                (! ((read1.charAt(i) == '/') && (read2.charAt(i) == '/')))) {
            i = i + 1;
        }
        return read1.substring(0,i);
    }

    public void run(Configuration conf) throws Exception {
        copyReadFilesToHdfs();
        log.info("Loaded " + numRecords + " records.");
        log.info("Filtered " + numFilteredRecords + " records due to trigram entropy filter.");
    }

}

