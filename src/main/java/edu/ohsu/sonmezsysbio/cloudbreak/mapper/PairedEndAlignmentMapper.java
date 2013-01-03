package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.zip.GZIPOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 7/17/12
 * Time: 5:27 PM
 */
public abstract class PairedEndAlignmentMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private static Logger logger = Logger.getLogger(PairedEndAlignmentMapper.class);

    private String localDir;
    protected Writer s1FileWriter;
    protected File s1File;
    protected Writer s2FileWriter;
    protected File s2File;
    protected OutputCollector<Text, Text> output;
    protected Reporter reporter;

    protected boolean getCompressTempReadFile() {
        return true;
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        this.localDir = job.get("mapred.child.tmp");
        try {
            if (getCompressTempReadFile()) {
                s1File = new File(localDir + "/temp1_sequence.fastq.gz").getAbsoluteFile();
                s1File.createNewFile();
                s1FileWriter = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(s1File)));

                s2File = new File(localDir + "/temp2_sequence.fastq.gz").getAbsoluteFile();
                s2File.createNewFile();
                s2FileWriter = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(s2File)));
            } else {
                s1File = new File(localDir + "/temp1_sequence.fastq").getAbsoluteFile();
                s1File.createNewFile();
                s1FileWriter = new OutputStreamWriter(new FileOutputStream(s1File));

                s2File = new File(localDir + "/temp2_sequence.fastq").getAbsoluteFile();
                s2File.createNewFile();
                s2FileWriter = new OutputStreamWriter(new FileOutputStream(s2File));
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (this.output == null) {
            this.output = output;
        }
        if (this.reporter == null) {
            this.reporter = reporter;
        }

        String line = value.toString();
        String[] reads = line.split("\n");
        splitRead(key, reads[0], s1FileWriter);
        splitRead(key, reads[1], s2FileWriter);

        reporter.progress();
        logger.debug("Done with map method, real work will happen in close");
    }

    private void splitRead(LongWritable key, String line, Writer fileWriter) throws IOException {
        String[] fields = line.split("\t");

        if (fields[1].length() != fields[3].length()) {
            logger.warn("Warning; mismatching seq and qual lengths in record " + key.toString() + "!");
            logger.warn("Seq:");
            logger.warn(fields[1]);
            logger.warn("Qual:");
            logger.warn(fields[3]);
            logger.warn("DONE WARNING");
        }
        fileWriter.write(fields[0] + "\n");
        fileWriter.write(fields[1] + "\n");
        fileWriter.write(fields[2] + "\n");
        fileWriter.write(fields[3] + "\n");
    }

    public OutputCollector<Text, Text> getOutput() {
        return output;
    }

    public void setOutput(OutputCollector<Text, Text> output) {
        this.output = output;
    }

    public Reporter getReporter() {
        return reporter;
    }

    public void setReporter(Reporter reporter) {
        this.reporter = reporter;
    }
}
