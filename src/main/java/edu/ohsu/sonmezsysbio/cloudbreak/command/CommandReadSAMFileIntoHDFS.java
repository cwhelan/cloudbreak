package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.io.HDFSWriter;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 10/15/12
 * Time: 10:50 AM
 */
@Parameters(separators = "=", commandDescription = "Load a SAM/BAM file into HDFS")
public class CommandReadSAMFileIntoHDFS implements CloudbreakCommand {

    private static org.apache.log4j.Logger logger = Logger.getLogger(CommandReadSAMFileIntoHDFS.class);

    @Parameter(names = {"--HDFSDataDir"}, required = true, description = "HDFS Directory to hold the alignment data")
    String hdfsDataDir;

    @Parameter(names = {"--samFile"}, required = true, description = "Path to the SAM/BAM file on the local filesystem")
    String samFile;

    @Parameter(names = {"--outFileName"}, description = "Filename to give the file in HDFS")
    String outFileName = "alignments";

    @Parameter(names = {"--compress"}, description = "Compression codec to use for the data")
    String compress = "snappy";


    public void run(Configuration conf) throws Exception {
        Configuration config = new Configuration();

        FileSystem hdfs = FileSystem.get(config);
        Path p = new Path(hdfsDataDir + "/" + outFileName);

        //HDFSWriter writer = getHdfsWriter(config, hdfs, p);

//        try {
//            readFile(writer, samFile);
//        } finally {
//            writer.close();
//        }

        readFile(null, samFile, config, hdfs);
    }

    static HDFSWriter getHdfsWriter(Configuration config, FileSystem hdfs, Path p, String compress) throws IOException {
        HDFSWriter writer = new HDFSWriter();
        if ("snappy".equals(compress)) {
            writer.seqFileWriter = SequenceFile.createWriter(hdfs, config, p, Text.class, Text.class,
                    131072, (short) 1, hdfs.getDefaultBlockSize(), true,
                    SequenceFile.CompressionType.BLOCK, new SnappyCodec(), new SequenceFile.Metadata());
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
        return writer;
    }

    private void readFile(HDFSWriter writer, String samFile, Configuration config, FileSystem hdfs) throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        SAMFileReader samFileReader = new SAMFileReader(new File(samFile));
        samFileReader.setValidationStringency(SAMFileReader.ValidationStringency.SILENT);
        SAMRecordIterator it = samFileReader.iterator();

        String currentReadName = "";
        int numRecords = 0;
        long lastTime = System.currentTimeMillis();
        Text key = new Text();
        KeyVal[] buffer = new KeyVal[10000000];
        while (it.hasNext()) {
            SAMRecord samRecord = it.next();
            String readName = samRecord.getReadName();
            currentReadName = readName;
            // key.set(currentReadName);
            // writer.write(key, samRecord.getSAMString());
            KeyVal keyVal = new KeyVal();
            keyVal.key = currentReadName;
            keyVal.val = samRecord.getSAMString();
            buffer[numRecords] = keyVal;
            numRecords++;
            if (numRecords % 10000000 == 0) {
                long currentTime = System.currentTimeMillis();
                System.err.println("Loaded " + numRecords + " in " + (currentTime - lastTime) + "ms");
                lastTime = currentTime;
                Path p = new Path(hdfsDataDir + "/" + outFileName + "-" + numRecords);
                UploadThread uploadThread = new UploadThread();
                uploadThread.recordNum = numRecords;
                uploadThread.buff = buffer;
                uploadThread.path = p;
                uploadThread.config = config;
                uploadThread.hdfs = hdfs;
                buffer = new KeyVal[10000000];
                executorService.execute(uploadThread);
            }
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {
        }

        System.err.println("Complete: loaded " + numRecords);
    }

    static class KeyVal {
        String key;
        String val;
    }

    class UploadThread implements Runnable {

        Path path;
        KeyVal[] buff;
        int recordNum;
        Configuration config;
        FileSystem hdfs;

        @Override
        public void run() {
            HDFSWriter writer = null;

            try {
                writer = getHdfsWriter(config, hdfs, path, CommandReadSAMFileIntoHDFS.this.compress);
                for (int i = 0; i < buff.length; i++) {
                    writer.write(new Text(buff[i].key), buff[i].val);
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Loaded " + recordNum);

        }
    }
}
