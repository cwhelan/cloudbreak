package edu.ohsu.sonmezsysbio.cloudbreak.io;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedWriter;
import java.io.IOException;

/**
* Created by IntelliJ IDEA.
* User: cwhelan
* Date: 1/4/13
* Time: 9:44 AM
*/

/**
 * This class is for writing files to HDFS, and hides whether the files are text files or sequence files
 * from its users
 */
public class HDFSWriter {
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
