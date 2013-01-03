package edu.ohsu.sonmezsysbio.cloudbreak.command;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/20/11
 * Time: 1:55 PM
 */
public interface CloudbreakCommand {
    public void run(Configuration conf) throws Exception;

    public static class HDFSWriter {
        public BufferedWriter textFileWriter;
        public SequenceFile.Writer seqFileWriter;

        public void write(Object key, String line) throws IOException {
            if (textFileWriter != null) {
                textFileWriter.write(line);
            } else {
                seqFileWriter.append(key, new Text(line));
            }
        }

        public void close() throws IOException {
            if (textFileWriter != null) {
                textFileWriter.close();
            } else {
                seqFileWriter.close();
            }
        }
    }
}
