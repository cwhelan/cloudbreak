package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
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

    public void run(Configuration conf) throws Exception {
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
                        String[] read1Alignments = reads[0].split(Cloudbreak.ALIGNMENT_SEPARATOR);
                        for (String read1Alignment : read1Alignments) {
                            System.out.println(read1Alignment);
                        }
                        String[] read2Alignments = reads[1].split(Cloudbreak.ALIGNMENT_SEPARATOR);
                        for (String read2Alignment : read2Alignments) {
                            System.out.println(read2Alignment);
                        }
                    }
                } finally {
                    reader.close();
                }
            }
        }

    }
}
