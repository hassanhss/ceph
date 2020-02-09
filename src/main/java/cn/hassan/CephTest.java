package cn.hassan;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;

public class CephTest {

    private static String accessKey = "0ORSZ5UVPR1FKUMFF8US";
    private static String secretKey = "lj2B4lrOv3JvS9P0I4jtwOUzCsnzNYHChCtwPLct";

    public static void main(String[] args) {

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);



        AmazonS3 conn = new AmazonS3Client(credentials, clientConfig);
        conn.setEndpoint("http://192.168.48.134:7480");

        //Bucket bucket = conn.createBucket("my-new-bucket");

//        List<Bucket> buckets = conn.listBuckets();
//        for (Bucket bucket : buckets) {
//            System.out.println(bucket.getName() + "\t" + StringUtils.fromDate(bucket.getCreationDate()));
//        }

//        ByteArrayInputStream input = new ByteArrayInputStream("Hello World!".getBytes());
//        conn.putObject("my-new-bucket", "hello.txt", input, new ObjectMetadata());

//        conn.getObject(
//                new GetObjectRequest("my-new-bucket", "hello.txt"),
//                new File("D:\\hello.txt")
//        );

        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest("my-new-bucket", "hello.txt");
        System.out.println(conn.generatePresignedUrl(request));
    }
}
