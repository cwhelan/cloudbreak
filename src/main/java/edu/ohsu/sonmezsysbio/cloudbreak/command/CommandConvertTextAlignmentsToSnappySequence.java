package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.SingleEndAlignmentsToReadPairInfoMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 9/4/12
 * Time: 10:22 PM
 */
@Parameters(separators = "=", commandDescription = "quickly covnvert text alignment output to a sequence file with snappy compression")
public class CommandConvertTextAlignmentsToSnappySequence implements CloudbreakCommand {

    @Parameter(names = {"--inputDir"}, required = true)
    String inputDir;

    @Parameter(names = {"--outputDir"}, required = true)
    String outputDir;

    public void run(Configuration conf) throws IOException, URISyntaxException {
        runHadoopJob(conf);
    }

    private void runHadoopJob(Configuration configuration) throws IOException, URISyntaxException {
        JobConf conf = new JobConf(configuration);

        conf.setJobName("Convert text alignments to sequence");
        conf.setJarByClass(Cloudbreak.class);

        FileSystem dfs = DistributedFileSystem.get(conf);

        FileInputFormat.addInputPath(conf, new Path(inputDir));
        Path outputDirPath = new Path(outputDir);

        FileSystem.get(conf).delete(outputDirPath);
        FileOutputFormat.setOutputPath(conf, outputDirPath);

        conf.setInputFormat(TextInputFormat.class);

        conf.setMapperClass(AlignmentLineToKeyValueMapper.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setReducerClass(IdentityReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setCompressMapOutput(true);

        conf.set("mapred.output.compress","true");
        conf.set("mapred.output.compression","org.apache.hadoop.io.compress.SnappyCodec");

        JobClient.runJob(conf);

    }

    public static class AlignmentLineToKeyValueMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        public AlignmentLineToKeyValueMapper() {
            super();
        }

        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> textTextOutputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            int firstTabIndex = line.indexOf('\t');
            String key = line.substring(0, firstTabIndex);
            String value = line.substring(firstTabIndex + 1);
            textTextOutputCollector.collect(new Text(key), new Text(value));
        }
    }
}
