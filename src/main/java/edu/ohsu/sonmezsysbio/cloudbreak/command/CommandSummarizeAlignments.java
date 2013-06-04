package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.SingleEndAlignmentSummaryMapper;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.SingleEndAlignmentSummaryReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/16/12
 * Time: 1:39 PM
 */
public class CommandSummarizeAlignments implements CloudbreakCommand {
    @Parameter(names = {"--inputHDFSDir"}, required = true)
    String inputHDFSDir;

    @Parameter(names = {"--aligner"})
    String aligner = Cloudbreak.ALIGNER_GENERIC_SAM;

    public void run(Configuration conf) throws Exception {
        runHadoopJob(conf);
    }

    private void runHadoopJob(Configuration configuration) throws IOException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("Summarize Single End Alignments");
        conf.setJarByClass(Cloudbreak.class);
        FileInputFormat.addInputPath(conf, new Path(inputHDFSDir));
        String outputHDFSDir = inputHDFSDir + "_cbtemp_summary";
        Path outputDir = new Path(outputHDFSDir);
        FileSystem.get(conf).delete(outputDir);

        FileOutputFormat.setOutputPath(conf, outputDir);

        conf.set("cloudbreak.aligner", aligner);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setMapperClass(SingleEndAlignmentSummaryMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setCombinerClass(SingleEndAlignmentSummaryReducer.class);

        conf.setReducerClass(SingleEndAlignmentSummaryReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setCompressMapOutput(true);

        JobClient.runJob(conf);

        BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path(outputDir + "/part-00000"))));
        String summaryLine = reader.readLine();
        String[] fields = summaryLine.split("\t");
        System.out.println("Reads\tAlignments\tBest Align Mismatches");
        System.out.println(fields[1] + "\t" + fields[2] + "\t" + fields[3]);

        FileSystem.get(conf).delete(outputDir);
    }
}
