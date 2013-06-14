package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Joiner;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 11/4/12
 * Time: 3:13 PM
 */
@Parameters(separators = "=", commandDescription = "Export alignments in SAM format")
public class CommandExportAlignmentsFromHDFS implements CloudbreakCommand {

    private static org.apache.log4j.Logger logger = Logger.getLogger(CommandExportAlignmentsFromHDFS.class);

    @Parameter(names = {"--inputHDFSDir"}, required = true)
    String inputHDFSDir;

    @Parameter(names = {"--aligner"})
    String aligner = Cloudbreak.ALIGNER_GENERIC_SAM;

    public void run(Configuration conf) throws Exception {
        if (Cloudbreak.ALIGNER_GENERIC_SAM.equals(aligner)) {
            System.err.println("Warnign: if you aligned these reads in single ended mode, you'll have to run samtools fixmate on the resulting bam file");
        }
        FileSystem dfs = DistributedFileSystem.get(conf);
        FileStatus[] stati = dfs.listStatus(new Path(inputHDFSDir));
        if (stati == null) {
            logger.error("Could not find input directory " + inputHDFSDir);
            return;
        }

        for (FileStatus s : stati) {
            if (s.getPath().getName().startsWith("part")) {
                Path path = s.getPath();
                logger.info(path);
                SequenceFile.Reader reader = null;
                try {
                    reader = new SequenceFile.Reader(dfs, path, conf);
                    Text key = new Text();
                    Text value = new Text();
                    while(reader.next(key, value)) {
                        String line = value.toString();
                        String[] reads = line.split(Cloudbreak.READ_SEPARATOR);
                        if (reads.length < 1 || reads.length > 2) {
                            throw new IllegalArgumentException("Bad line: " + line);
                        }
                        String[] read1Alignments = reads[0].split(Cloudbreak.ALIGNMENT_SEPARATOR);
                        for (String read1Alignment : read1Alignments) {
                            // fix the flags to show this a is a paired read
                            if (! read1Alignment.equals("")) {
                                if (Cloudbreak.ALIGNER_GENERIC_SAM.equals(aligner)) {
                                    String[] fields = read1Alignment.split("\t");
                                    if (fields.length < 2) {
                                        throw new IllegalArgumentException("Bad alignment record for key " + key.toString() + ": " + read1Alignment);
                                    }
                                    addPairedFlag(fields[1]);
                                    System.out.println(Joiner.on("\t").join(fields));
                                } else {
                                    System.out.println(read1Alignment);
                                }
                            }
                        }
                        if (reads.length == 2) {
                            String[] read2Alignments = reads[1].split(Cloudbreak.ALIGNMENT_SEPARATOR);
                            for (String read2Alignment : read2Alignments) {
                                if (! "".equals(read2Alignment)) {
                                    // fix the flags to show this a is a paired read
                                    if (Cloudbreak.ALIGNER_GENERIC_SAM.equals(aligner)) {
                                        String[] fields = read2Alignment.split("\t");
                                        addPairedFlag(fields[1]);
                                        System.out.println(Joiner.on("\t").join(fields));
                                    } else {
                                        System.out.println(read2Alignment);
                                    }
                                }
                            }
                        }
                    }
                } finally {
                    reader.close();
                }
            }
        }

    }

    private void addPairedFlag(String field) {
        int flag = Integer.parseInt(field);
        flag = flag | 0x1;
        field = String.valueOf(flag);
    }
}
