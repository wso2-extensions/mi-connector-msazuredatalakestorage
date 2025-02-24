package org.wso2.carbon.connector.operations;

import com.azure.core.util.BinaryData;
import com.azure.storage.common.ParallelTransferOptions;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.options.FileParallelUploadOptions;

import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;
import org.wso2.carbon.connector.util.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;


public class UploadFile extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String fileSystemName = getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String filePathToUpload = getMediatorParameter(messageContext, AzureConstants.FILE_PATH_TO_UPLOAD, String.class, false);
        String localFilePath = getMediatorParameter(messageContext, AzureConstants.LOCAL_FILE_PATH, String.class, false);
        String textContent = getMediatorParameter(messageContext, AzureConstants.TEXT_CONTENT, String.class, localFilePath != null );
        String metadata = getMediatorParameter(messageContext, AzureConstants.METADATA, String.class, true);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);
        String leaseId = getMediatorParameter(messageContext, AzureConstants.LEASE_ID, String.class, true);
        String ifUnmodifiedSince = getMediatorParameter(messageContext, AzureConstants.IF_UNMODIFIED_SINCE, String.class, true);
        String ifMatch = getMediatorParameter(messageContext, AzureConstants.IF_MATCH, String.class, true);
        String ifModifiedSince = getMediatorParameter(messageContext, AzureConstants.IF_MODIFIED_SINCE, String.class, true);
        String ifNoneMatch = getMediatorParameter(messageContext, AzureConstants.IF_NONE_MATCH, String.class, true);
        String contentLanguage = getMediatorParameter(messageContext, AzureConstants.CONTENT_LANGUAGE, String.class, true);
        String contentType = getMediatorParameter(messageContext, AzureConstants.CONTENT_TYPE, String.class, true);
        String contentEncoding = getMediatorParameter(messageContext, AzureConstants.CONTENT_ENCODING, String.class, true);
        String contentDisposition = getMediatorParameter(messageContext, AzureConstants.CONTENT_DISPOSITION, String.class, true);
        String cacheControl = getMediatorParameter(messageContext, AzureConstants.CACHE_CONTROL, String.class, true);
        Integer blockSize = getMediatorParameter(messageContext, AzureConstants.BLOCK_SIZE, Integer.class, true);
        Integer maxSingleUploadSize = getMediatorParameter(messageContext, AzureConstants.MAX_SINGLE_UPLAOD_SIZE, Integer.class, true);
        Integer maxConcurrency = getMediatorParameter(messageContext, AzureConstants.MAX_CONCURRENCY, Integer.class, true);

        Long maxSingleUploadSizeL = maxSingleUploadSize != null ? maxSingleUploadSize.longValue() * 1024L * 1024L : null;
        Long blockSizeL = blockSize != null ? blockSize.longValue() * 1024L * 1024L : null;



        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        try {
            AzureStorageConnectionHandler azureStorageConnectionHandler = (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME, connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient(fileSystemName);
            DataLakeFileClient dataLakeFileClient = dataLakeFileSystemClient.getFileClient(filePathToUpload);

            ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
                    .setBlockSizeLong(blockSizeL)
                    .setMaxConcurrency(maxConcurrency)
                    .setMaxSingleUploadSizeLong(maxSingleUploadSizeL);

            PathHttpHeaders headers = new PathHttpHeaders()
                    .setCacheControl(cacheControl)
                    .setContentType(contentType)
                    .setContentDisposition(contentDisposition)
                    .setContentEncoding(contentEncoding)
                    .setContentLanguage(contentLanguage);

            DataLakeRequestConditions requestConditions = new DataLakeRequestConditions()
                    .setLeaseId(leaseId)
                    .setIfMatch(ifMatch)
                    .setIfModifiedSince(ifModifiedSince != null ? OffsetDateTime.parse(ifModifiedSince) : null)
                    .setIfNoneMatch(ifNoneMatch)
                    .setIfUnmodifiedSince(ifUnmodifiedSince != null ? OffsetDateTime.parse(ifUnmodifiedSince) : null);

            HashMap<String, String> metadataMap = new HashMap<>();


            if (metadata != null && !metadata.isEmpty()) {
                Utils.addDataToMapFromJsonString(metadata, metadataMap);
            }


            if(localFilePath != null){
                byte[] fileContent = Files.readAllBytes(Paths.get(localFilePath));


                dataLakeFileClient.uploadFromFileWithResponse(
                        localFilePath,
                        parallelTransferOptions,
                        headers.setContentMd5(MessageDigest.getInstance("MD5").digest(fileContent)),
                        metadataMap,
                        requestConditions,
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null,
                        null
                );
            }else if(textContent != null){
                dataLakeFileClient.uploadWithResponse(
                        new FileParallelUploadOptions(BinaryData.fromString(textContent))
                                .setHeaders(headers.setContentMd5(MessageDigest.getInstance("MD5").digest(textContent.getBytes())))
                                .setParallelTransferOptions(parallelTransferOptions)
                                .setMetadata(metadataMap).setRequestConditions(requestConditions),
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null,
                        null
                );
            }

            handleConnectorResponse(messageContext, responseVariable, overwriteBody, true, null, null);
        } catch (DataLakeStorageException e) {
            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);
        } catch ( NoSuchAlgorithmException e){
            handleConnectorException(Error.No_SUCH_ALGORITHM, messageContext, e);
        } catch ( IOException e){
            handleConnectorException(Error.IO_EXCEPTION, messageContext, e);
        } catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }
    }
}
