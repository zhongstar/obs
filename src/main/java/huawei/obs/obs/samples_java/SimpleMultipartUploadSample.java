package huawei.obs.obs.samples_java;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.UUID;

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AuthTypeEnum;
import com.obs.services.model.CompleteMultipartUploadRequest;
import com.obs.services.model.InitiateMultipartUploadRequest;
import com.obs.services.model.InitiateMultipartUploadResult;
import com.obs.services.model.PartEtag;
import com.obs.services.model.UploadPartResult;


/**
 * This sample demonstrates how to upload multiparts to OBS 
 * using the OBS SDK for Java.
 */
public class SimpleMultipartUploadSample
{
    private static final String endPoint = "https://obs.cn-north-1.myhuaweicloud.com";

    private static final String ak = "YDPOHSJ5O1UIGC1NB1KV";

    private static final String sk = "gVuBA1TkoMQep4K1KlLKn4r8kgrCOFPC6DesHrq4";

    private static ObsClient obsClient;

    private static String bucketName = "zhong-bucket";
    
    private static String objectKey = "my-obs-object-key-demo";
    
    public static void main(String[] args) throws IOException
    {
        ObsConfiguration config = new ObsConfiguration();
        config.setSocketTimeout(30000);
        config.setConnectionTimeout(10000);
        config.setEndPoint(endPoint);
        try
        {
            /*
             * Constructs a obs client instance with your account for accessing OBS
             */
            obsClient = new ObsClient(ak, sk, config);
            
            /*
             * Create bucket 
             */
            obsClient.createBucket(bucketName);
            
            /*
             * Step 1: initiate multipart upload
             */
            System.out.println("Step 1: initiate multipart upload \n");
            InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest();
            request.setBucketName(bucketName);
            request.setObjectKey(objectKey);
            InitiateMultipartUploadResult result = obsClient.initiateMultipartUpload(request);
            
            /*
             * Step 2: upload a part
             */
            System.out.println("Step 2: upload part \n");
            UploadPartResult uploadPartResult = obsClient.uploadPart(bucketName, objectKey, result.getUploadId(), 1, new FileInputStream(createSampleFile()));
            
            /*
             * Step 3: complete multipart upload
             */
            System.out.println("Step 3: complete multipart upload \n");
            CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest();
            completeMultipartUploadRequest.setBucketName(bucketName);
            completeMultipartUploadRequest.setObjectKey(objectKey);
            completeMultipartUploadRequest.setUploadId(result.getUploadId());
            PartEtag partEtag = new PartEtag();
            partEtag.setPartNumber(uploadPartResult.getPartNumber());
            partEtag.seteTag(uploadPartResult.getEtag());
            completeMultipartUploadRequest.getPartEtag().add(partEtag);
            obsClient.completeMultipartUpload(completeMultipartUploadRequest);
        }
        catch (ObsException e)
        {
            System.out.println("Response Code: " + e.getResponseCode());
            System.out.println("Error Message: " + e.getErrorMessage());
            System.out.println("Error Code:       " + e.getErrorCode());
            System.out.println("Request ID:      " + e.getErrorRequestId());
            System.out.println("Host ID:           " + e.getErrorHostId());
        }
        finally
        {
            if (obsClient != null)
            {
                try
                {
                    /*
                     * Close obs client 
                     */
                    obsClient.close();
                }
                catch (IOException e)
                {
                }
            }
        }
        
    }
    
    private static File createSampleFile()
        throws IOException
    {
        File file = File.createTempFile("obs-java-sdk-", ".txt");
        file.deleteOnExit();
        
        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        for (int i = 0; i < 1000000; i++)
        {
            writer.write(UUID.randomUUID() + "\n");
            writer.write(UUID.randomUUID() + "\n");
        }
        writer.flush();
        writer.close();
        
        return file;
    }
}
