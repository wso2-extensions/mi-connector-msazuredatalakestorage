package org.wso2.carbon.connector.operations;

import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.LeaseAction;
import com.azure.storage.file.datalake.options.DataLakeFileAppendOptions;
import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.Duration;

public class AppendFile extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {

        String connectionName = getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String fileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String filePathToAppend =
                getMediatorParameter(messageContext, AzureConstants.FILE_PATH_TO_APPEND, String.class, false);
        String inputType = getMediatorParameter(messageContext, AzureConstants.INPUT_TYPE, String.class, false);
        String localFilePath =
                getMediatorParameter(messageContext, AzureConstants.LOCAL_FILE_PATH, String.class,
                        !inputType.equals(AzureConstants.L_LOCAL_FILE_PATH));
        String textContent =
                getMediatorParameter(messageContext, AzureConstants.TEXT_CONTENT, String.class,
                        !inputType.equals(AzureConstants.L_TEXT_CONTENT));
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);
        Boolean flush = getMediatorParameter(messageContext, AzureConstants.FLUSH, Boolean.class, true);
        String leaseId = getMediatorParameter(messageContext, AzureConstants.LEASE_ID, String.class, true);
        String leaseAction = getMediatorParameter(messageContext, AzureConstants.LEASE_ACTION, String.class, true);
        Integer leaseDuration =
                getMediatorParameter(messageContext, AzureConstants.LEASE_DURATION, Integer.class, true);
        String proposedLeaseId =
                getMediatorParameter(messageContext, AzureConstants.PROPOSED_LEASE_ID, String.class, true);

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        LeaseAction leaseActionConstant = getLeaseAction(leaseAction);
        Response<?> response = null;

        try {
            AzureStorageConnectionHandler
                    azureStorageConnectionHandler =
                    (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME,
                            connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            DataLakeFileSystemClient dataLakeFileSystemClient =
                    dataLakeServiceClient.getFileSystemClient(fileSystemName);
            DataLakeFileClient dataLakeFileClient = dataLakeFileSystemClient.getFileClient(filePathToAppend);
            long fileSize = dataLakeFileClient.getProperties().getFileSize();
            if (textContent != null && localFilePath == null) {
                response = dataLakeFileClient.appendWithResponse(
                        BinaryData.fromString(textContent),
                        fileSize,
                        new DataLakeFileAppendOptions()
                                .setFlush(flush)
                                .setContentHash(
                                        MessageDigest.getInstance("MD5").digest(textContent.getBytes())
                                               )
                                .setLeaseId(leaseId)
                                .setLeaseAction(leaseActionConstant)
                                .setLeaseDuration(leaseDuration),
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null,
                        null);
            } else if (localFilePath != null && textContent == null) {
                Path filePath = Paths.get(localFilePath);
                byte[] fileContent = Files.readAllBytes(filePath);
                response = dataLakeFileClient.appendWithResponse(
                        BinaryData.fromFile(filePath),
                        fileSize,
                        new DataLakeFileAppendOptions().setFlush(flush)
                                .setContentHash(MessageDigest.getInstance("MD5").digest(fileContent))
                                .setLeaseId(leaseId)
                                .setLeaseAction(leaseActionConstant)
                                .setLeaseDuration(leaseDuration)
                                .setProposedLeaseId(proposedLeaseId)
                        ,
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null,
                        null);
            }

            if (response != null && response.getStatusCode() == 202) {
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, true, null, null);
            } else {
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, false, null, null);
            }

        } catch (DataLakeStorageException e) {
            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);
        } catch (ConnectException e) {
            handleConnectorException(Error.CONNECTION_ERROR, messageContext, e);
        } catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }

    }

}
