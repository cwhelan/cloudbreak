package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import edu.ohsu.sonmezsysbio.cloudbreak.cloud.S3Uploader;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/29/13
 * Time: 10:43 AM
 */
public class CommandCopyToS3 implements CloudbreakCommand {

    @Parameter(names = {"--fileName"}, required = true)
    String fileName;

    @Parameter(names = {"--S3Bucket"}, required = true)
    String s3bucket;

    @Override
    public void run(Configuration conf) throws Exception {
        new S3Uploader(s3bucket, fileName).uploadToS3();
    }


}
