package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Joiner;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 10/15/12
 * Time: 10:50 AM
 */
@Parameters(separators = "=", commandDescription = "Load paired fastq files into HDFS")
public class CommandReadSAMFileIntoHDFS implements CloudbreakCommand {

    private static org.apache.log4j.Logger logger = Logger.getLogger(CommandReadSAMFileIntoHDFS.class);

    @Parameter(names = {"--HDFSDataDir"}, required = true)
    String hdfsDataDir;

    @Parameter(names = {"--samFile"}, required = true)
    String samFile;

    @Parameter(names = {"--outFileName"})
    String outFileName = "reads.txt";

    @Parameter(names = {"--compress"})
    String compress = "snappy";


    public void run(Configuration conf) throws Exception {
        Configuration config = new Configuration();

        FileSystem hdfs = FileSystem.get(config);
        Path p = new Path(hdfsDataDir + "/" + outFileName);

        HDFSWriter writer = new HDFSWriter();
        if ("snappy".equals(compress)) {
            writer.seqFileWriter = SequenceFile.createWriter(hdfs, config, p, Text.class, Text.class, SequenceFile.CompressionType.BLOCK, new SnappyCodec());
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
            readFile(writer, samFile);
        } finally {
            writer.close();
        }

    }
    private void readFile(HDFSWriter writer, String samFile) throws IOException {
        SAMFileReader samFileReader = new SAMFileReader(new File(samFile));
        samFileReader.setValidationStringency(SAMFileReader.ValidationStringency.LENIENT);
        SAMRecordIterator it = samFileReader.iterator();

        String currentReadName = "";

        while (it.hasNext()) {
            SAMRecord samRecord = it.next();
            String readName = samRecord.getReadName();
            writer.write(new Text(currentReadName), samRecord.getSAMString());
        }
    }

}
