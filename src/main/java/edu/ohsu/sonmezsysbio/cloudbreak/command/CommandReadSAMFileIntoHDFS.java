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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        HDFSWriter writer = Cloudbreak.getHdfsWriter(config, hdfs, p, compress);
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
        List<String> read1Records = new ArrayList<String>();
        List<String> read2Records = new ArrayList<String>();
        Joiner readAlignmentJoiner = Joiner.on(Cloudbreak.ALIGNMENT_SEPARATOR);
        long i = 0;
        while (it.hasNext()) {
            SAMRecord samRecord = it.next();
            // todo check sorting against previous record
            String readName = samRecord.getReadName();
            if (! readName.equals(currentReadName) && ! currentReadName.equals("")) {
                logger.debug("writing " + readName);
                writeRecords(currentReadName, writer, read1Records, read2Records, readAlignmentJoiner, i);
                currentReadName = readName;
                i++;
            }

            if (currentReadName.equals("")) {
                currentReadName = readName;
            }
            if (samRecord.getReadPairedFlag() && ! samRecord.getReadUnmappedFlag()) {
                if (samRecord.getFirstOfPairFlag()) {
                    read1Records.add(samRecord.getSAMString().trim());
                } else {
                    read2Records.add(samRecord.getSAMString().trim());
                }
            }
        }
        writeRecords(currentReadName, writer, read1Records, read2Records, readAlignmentJoiner, i);
    }

    private void writeRecords(String currentReadName, HDFSWriter writer, List<String> read1Records, List<String> read2Records, Joiner readAlignmentJoiner, long i) throws IOException {
        logger.debug("r1 records: " + read1Records.size());
        logger.debug("r2 records: " + read1Records.size());
        // todo: not importing OEA mappings yet
        if (read1Records.size() > 0 && read2Records.size() > 0) {
            writer.write(new Text(currentReadName), readAlignmentJoiner.join(read1Records) + Cloudbreak.READ_SEPARATOR + readAlignmentJoiner.join(read2Records) + "\n");
        }
        read1Records.clear();
        read2Records.clear();
    }
}
