package huawei.obs.obs.samples_java;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ObjectMetadata;

/**
 * This sample demonstrates how to set/get self-defined metadata for object
 * on OBS using the OBS SDK for Java.
 */
public class ObjectMetaSample
{
    private static final String endPoint = "https://obs.cn-north-1.myhuaweicloud.com";

    private static final String ak = "YDPOHSJ5O1UIGC1NB1KV";

    private static final String sk = "gVuBA1TkoMQep4K1KlLKn4r8kgrCOFPC6DesHrq4";

    private static ObsClient obsClient;

    private static String bucketName = "zhong-bucket";
    
    private static String objectKey = "my-obs-object-key-demo";
    
    public static void main(String[] args)
        throws UnsupportedEncodingException
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
            System.out.println("Create a new bucket for demo\n");
            obsClient.createBucket(bucketName);
            
            ObjectMetadata meta = new ObjectMetadata();
            /*
             * Setting object mime type
             */
            meta.setContentType("text/plain");
            /*
             * Setting self-defined metadata
             */
            meta.getMetadata().put("meta1", "value1");
            meta.addUserMetadata("meta2", "value2");
            String content = "Hello OBS";
            obsClient.putObject(bucketName, objectKey, new ByteArrayInputStream(content.getBytes("UTF-8")), meta);
            System.out.println("Create object " + objectKey + " successfully!\n");
            
            /*
             * Get object metadata
             */
            ObjectMetadata result = obsClient.getObjectMetadata(bucketName, objectKey);
            System.out.println("Getting object metadata:");
            System.out.println("\tContentType:" + result.getContentType());
            System.out.println("\tmeta1:" + result.getUserMetadata("meta1"));
            System.out.println("\tmeta2:" + result.getUserMetadata("meta2"));
            
            /*
             * Delete object
             */
            obsClient.deleteObject(bucketName, objectKey);
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
}
