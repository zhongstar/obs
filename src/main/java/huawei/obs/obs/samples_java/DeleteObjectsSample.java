package huawei.obs.obs.samples_java;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.exception.ObsException;
import com.obs.services.model.DeleteObjectsRequest;
import com.obs.services.model.DeleteObjectsResult;
import com.obs.services.model.DeleteObjectsResult.DeleteObjectResult;
import com.obs.services.model.DeleteObjectsResult.ErrorResult;
import com.obs.services.model.KeyAndVersion;

/**
 * This sample demonstrates how to delete objects under specified bucket 
 * from OBS using the OBS SDK for Java.
 */
public class DeleteObjectsSample
{
    private static final String endPoint = "https://obs.cn-north-1.myhuaweicloud.com";

    private static final String ak = "YDPOHSJ5O1UIGC1NB1KV";

    private static final String sk = "gVuBA1TkoMQep4K1KlLKn4r8kgrCOFPC6DesHrq4";

    private static ObsClient obsClient;

    private static String bucketName = "zhong-bucket";
    
    public static void main(String[] args)
        throws IOException
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
            
            /*
             * Batch put objects into the bucket
             */
            final String content = "Thank you for using Object Storage Service";
            final String keyPrefix = "MyObjectKey";
            List<String> keys = new ArrayList<String>();
            for (int i = 0; i < 100; i++)
            {
                String key = keyPrefix + i;
                InputStream instream = new ByteArrayInputStream(content.getBytes());
                obsClient.putObject(bucketName, key, instream, null);
                System.out.println("Succeed to put object " + key);
                keys.add(key);
            }
            System.out.println();
            
            /*
             * Delete all objects uploaded recently under the bucket
             */
            System.out.println("\nDeleting all objects\n");
            
            DeleteObjectsRequest request = new DeleteObjectsRequest();
            request.setBucketName(bucketName);
            request.setQuiet(false);
            
            KeyAndVersion[] kvs = new KeyAndVersion[keys.size()];
            int index = 0;
            for (String key : keys)
            {
                kvs[index++] = new KeyAndVersion(key);
            }
            
            request.setKeyAndVersions(kvs);
            
            System.out.println("Delete results:");
            
            DeleteObjectsResult deleteObjectsResult = obsClient.deleteObjects(request);
            for (DeleteObjectResult object : deleteObjectsResult.getDeletedObjectResults())
            {
                System.out.println("\t" + object);
            }
            
            System.out.println("Error results:");
            
            for (ErrorResult error : deleteObjectsResult.getErrorResults())
            {
                System.out.println("\t" + error);
            }
            
            System.out.println();
            
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
