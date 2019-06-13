package project.storage.s3.impl;

import java.util.List;
import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;

import project.storage.StorageFactory;
import org.apache.commons.io.IOUtils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.util.StringUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import project.storage.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3 implements Storage {

    private static final Logger logger = LoggerFactory.getLogger(S3.class);
    private static final String DEFAULT_S3_ENDPOINT_URL = "s3-eu-west-1.amazonaws.com";

    AmazonS3 conn = null;
    Bucket bucket = null;

    protected void _init(
            String accessKey,
            String secretKey,
            String bucketName,
            String endpointURL) {

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);

        conn = new AmazonS3Client(credentials, clientConfig);
        conn.setEndpoint(endpointURL);

        List<Bucket> buckets = conn.listBuckets();
        for (Bucket b : buckets) {
            if (b.getName().equals(bucketName)) {
                bucket = b;
                break;
            }
        }

        if (bucket == null) {
            throw new NullPointerException("bucket '" + bucketName + "' not found");
        }
    }
    public S3(Properties properties) throws IllegalArgumentException {

        String aws_access_key_id = properties.getProperty("aws_access_key_id");
        if (aws_access_key_id == null) {
            throw new IllegalArgumentException("missing required parameter 'aws_access_key_id'");
        }
        String aws_secret_access_key = properties.getProperty("aws_secret_access_key");
        if (aws_secret_access_key == null) {
            throw new IllegalArgumentException("missing required parameter 'aws_secret_access_key'");
        }
        String s3_bucket_name = properties.getProperty("s3_bucket_name");
        if (s3_bucket_name == null) {
            throw new IllegalArgumentException("missing required parameter 's3_bucket_name'");
        }
        String s3_endpoint_url = properties.getProperty("s3_endpoint_url", DEFAULT_S3_ENDPOINT_URL);
        _init(aws_access_key_id, aws_secret_access_key, s3_bucket_name, s3_endpoint_url);
    }

    public S3(String accessKey, String secretKey, String bucketName) {
        _init(accessKey, secretKey, bucketName, DEFAULT_S3_ENDPOINT_URL);
    }

    public void list() {
        System.out.println("listing objects for bucket: '" + bucket.getName() + "'");

        ObjectListing objects = conn.listObjects(bucket.getName());
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                System.out.println(
                        objectSummary.getKey() + "\t"
                        + objectSummary.getSize() + "\t"
                        + StringUtils.fromDate(objectSummary.getLastModified()));
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
    }

    public String value(String key) throws IllegalArgumentException {
        S3Object object = null;
        try {
            object = conn.getObject(new GetObjectRequest(bucket.getName(), key));
        } catch (AmazonS3Exception ae) {
            throw new IllegalArgumentException("no value found for key '" + key + "'", ae);
        }

        logger.debug("read value for key '" + key + "'");

        InputStream objData = object.getObjectContent();
        String result = null;
        try {
            result = IOUtils.toString(objData, "UTF-8");
            objData.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public void put(String key, String value) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength((long) value.length());

        logger.debug("sending new data element");
        logger.debug("\tkey: '" + key + "'");
        logger.debug("\tvalue: " + value.length() + " bytes");

        conn.putObject(
                bucket.getName(),
                key,
                IOUtils.toInputStream(value),
                metadata);

        logger.debug("... data element saved");

    }

//    public void list() {
//        if (this.conn == null) {
//            connect();
//        }
//
//        List<Bucket> buckets = conn.listBuckets();
//        for(Bucket bucket: buckets) {
//            System.out.println(bucket.getName() + "\t" + StringUtils.fromDate(bucket.getCreationDate()));
//            ObjectListing objects = conn.listObjects(bucket.getName());
//            do {
//                for (S3ObjectSummary objectSummary: objects.getObjectSummaries()) {
//                    System.out.println(
//                        objectSummary.getKey() + "\t"
//                        + objectSummary.getSize() + "\t"
//                        + StringUtils.fromDate(objectSummary.getLastModified()));
//                }
//                objects = conn.listNextBatchOfObjects(objects);
//            } while(objects.isTruncated());
//        }
//    }

}
