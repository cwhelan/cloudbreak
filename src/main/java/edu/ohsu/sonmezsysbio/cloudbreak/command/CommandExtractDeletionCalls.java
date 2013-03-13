package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.mapper.GMMResultsToChromosomeMapper;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GMMResultsToVariantCallsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/22/12
 * Time: 10:33 PM
 */
@Parameters(separators = "=", commandDescription = "Extract deletion calls into a BED file")
public class CommandExtractDeletionCalls extends VariantExtractionCommand implements CloudbreakCommand {

    @Override
    protected String getVariantType() {
        return Cloudbreak.VARIANT_TYPE_DELETION;
    }

    @Override
    public void run(Configuration conf) throws Exception {
        runHadoopJob(conf);
    }

}
