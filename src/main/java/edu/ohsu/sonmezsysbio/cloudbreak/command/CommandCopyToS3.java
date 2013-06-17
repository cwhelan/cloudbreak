package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.cloud.S3Uploader;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/29/13
 * Time: 10:43 AM
 */

@Parameters(commandDescription = "Upload a file to Amazon S3 using multi-part upload")
public class CommandCopyToS3 implements CloudbreakCommand {

    @Parameter(names = {"--fileName"}, required = true, description = "Path to the file to be uploaded on the local filesystem")
    String fileName;

    @Parameter(names = {"--S3Bucket"}, required = true, description = "S3 Bucket to upload to")
    String s3bucket;

    @Override
    public void run(Configuration conf) throws Exception {
        new S3Uploader(s3bucket, fileName).uploadToS3();
    }


}
