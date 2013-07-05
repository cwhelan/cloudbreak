package edu.ohsu.sonmezsysbio.cloudbreak.cloud;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ProgressEvent;
import com.amazonaws.services.s3.model.ProgressListener;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/30/13
 * Time: 11:07 AM
 */
public class S3Uploader {

    private String bucketname;
    private String filename;
    private Upload upload;
    private String key;

    public S3Uploader(String bucketname, String filename, String dest) {
        this.bucketname = bucketname;
        this.filename = filename;
        this.key = dest;
    }

    public void uploadToS3() throws InterruptedException {
        //AmazonS3 s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider("whirr/cloudbreak-whirr.properties"));
        AmazonS3 s3 = new AmazonS3Client(new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                String credentialsFilePath = "/whirr/cloudbreak-whirr.properties";
                InputStream inputStream = getClass().getResourceAsStream(credentialsFilePath);
                if (inputStream == null) {
                    throw new AmazonClientException("Unable to load AWS credentials from the " + credentialsFilePath + " file on the classpath");
                }
                Properties accountProperties = new Properties();
                try {

                    accountProperties.load(inputStream);
                    final String accessKeyId = accountProperties.getProperty("whirr.identity");
                    final String secretKey = accountProperties.getProperty("whirr.credential");
                    return new AWSCredentials() {
                        @Override
                        public final String getAWSAccessKeyId() {
                            return accessKeyId;
                        }

                        @Override
                        public final String getAWSSecretKey() {
                            return secretKey;
                        }
                    };
                } catch (IOException e) {
                    throw new AmazonClientException("Unable to load AWS credentials from the " + credentialsFilePath + " file on the classpath", e);
                }

            }

            @Override
            public void refresh() {
            }
        });

        try {
            System.err.println("Uploading a new object to S3 from file: " + filename + "\n");

            TransferManager tm = new TransferManager(s3);
            File file = new File(filename);

            PutObjectRequest request = new PutObjectRequest(bucketname, key, file).withProgressListener(new ProgressListener() {
                int percentComplete = 0;

                @Override
                public void progressChanged(ProgressEvent progressEvent) {
                    if (upload == null) return;
                    if (percentComplete != (int) upload.getProgress().getPercentTransfered()) {
                        percentComplete = (int) upload.getProgress().getPercentTransfered();
                        System.err.println("Percent transferred: " + percentComplete);
                    }
                }
            });
            upload = tm.upload(request);
            UploadResult result = upload.waitForUploadResult();

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

}
