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
 * Date: 6/12/13
 * Time: 4:17 PM
 */
public abstract class AlignerMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private static org.apache.log4j.Logger logger = Logger.getLogger(SingleEndAlignerMapper.class);
    protected String localDir;

    protected File s1File;
    protected File s2File;
    protected Writer s1FileWriter;
    protected Writer s2FileWriter;
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
            FileAndWriter faw = openFileWriter(localDir, 1);
            s1File = faw.file;
            s1FileWriter = faw.writer;
            faw = openFileWriter(localDir, 2);
            s2File = faw.file;
            s2FileWriter = faw.writer;
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    protected int getNumRunningMappers() throws IOException {
        int numRunningMappers = 0;
        Runtime runtime = Runtime.getRuntime();
        String cmds[] = {"ps", "-C", getCommandName(), "--no-headers"};
        Process proc = runtime.exec(cmds);
        InputStream inputstream = proc.getInputStream();
        InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
        BufferedReader bufferedreader = new BufferedReader(inputstreamreader);
        while (bufferedreader.readLine() != null) {
            numRunningMappers++;
        }

        return numRunningMappers;
    }

    protected abstract String getCommandName();

    protected void waitForOpenSlot(int maxProcessesOnNode, Reporter reporter) throws IOException, InterruptedException {
        while (true) {
            // sleep for a random length of time between 0 and 60 seconds
            long sleepTime = (long) (Math.random() * 1000 * 60);
            logger.info("sleeping for " + sleepTime);
            Thread.sleep(sleepTime);
            int numRunningMappers = getNumRunningMappers();
            logger.info("num running mappers: " + numRunningMappers);
            if (numRunningMappers < maxProcessesOnNode) return;
            reporter.progress();
        }
    }

    static class FileAndWriter {
        File file;
        Writer writer;

        FileAndWriter(File file, Writer writer) {
            this.file = file;
            this.writer = writer;
        }
    }

    protected FileAndWriter openFileWriter(String localDir, int fileNum) throws IOException {
        File outFile;
        Writer outWriter;
        if (getCompressTempReadFile()) {
            outFile = new File(localDir + "/temp" + fileNum + "_sequence.fastq.gz").getAbsoluteFile();
            outFile.createNewFile();
            outWriter = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outFile)));
        } else {
            outFile = new File(localDir + "/temp" + fileNum + "_sequence.fastq").getAbsoluteFile();
            outFile.createNewFile();
            outWriter = new OutputStreamWriter(new FileOutputStream(outFile));
        }

        return new FileAndWriter(outFile, outWriter);
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
        splitRead(key, reads[1], getRead2FileWriter());

        reporter.progress();
    }

    protected abstract Writer getRead2FileWriter();

    private void splitRead(LongWritable key, String line, Writer writer) throws IOException {
        String[] fields = line.split("\t");

        if (fields[1].length() != fields[3].length()) {
            logger.warn("Warning; mismatching seq and qual lengths in record " + key.toString() + "!");
            logger.warn("Seq:");
            logger.warn(fields[1]);
            logger.warn("Qual:");
            logger.warn(fields[3]);
            logger.warn("DONE WARNING");
        }
        writer.write(fields[0] + "\n");
        writer.write(fields[1] + "\n");
        writer.write(fields[2] + "\n");
        writer.write(fields[3] + "\n");
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

    @Override
    public void close() throws IOException {
        super.close();
        if (s1FileWriter != null) s1FileWriter.close();
        if (s2FileWriter != null) s2FileWriter.close();
    }
}
