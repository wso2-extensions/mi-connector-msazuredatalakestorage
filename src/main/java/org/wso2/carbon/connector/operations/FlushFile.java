package org.wso2.carbon.connector.operations;

import com.azure.core.http.rest.Response;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.LeaseAction;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.options.DataLakeFileFlushOptions;
import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

import java.time.Duration;

/**
 * Implements the flush file operation.
 */
public class FlushFile extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {

        String connectionName = getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String fileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String filePathToFlush =
                getMediatorParameter(messageContext, AzureConstants.FILE_PATH_TO_FLUSH, String.class, false);
        String contentLanguage =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_LANGUAGE, String.class, true);
        String ifMatch = getMediatorParameter(messageContext, AzureConstants.IF_MATCH, String.class, true);
        String ifNoneMatch = getMediatorParameter(messageContext, AzureConstants.IF_NONE_MATCH, String.class, true);
        String ifModifiedSince =
                getMediatorParameter(messageContext, AzureConstants.IF_MODIFIED_SINCE, String.class, true);
        String ifUnmodifiedSince =
                getMediatorParameter(messageContext, AzureConstants.IF_UNMODIFIED_SINCE, String.class, true);
        String contentType = getMediatorParameter(messageContext, AzureConstants.CONTENT_TYPE, String.class, true);
        String contentDisposition =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_DISPOSITION, String.class, true);
        String contentEncoding =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_ENCODING, String.class, true);
        String cacheControl = getMediatorParameter(messageContext, AzureConstants.CACHE_CONTROL, String.class, true);
        String leaseId = getMediatorParameter(messageContext, AzureConstants.LEASE_ID, String.class, true);
        String proposedLeaseId =
                getMediatorParameter(messageContext, AzureConstants.PROPOSED_LEASE_ID, String.class, true);
        Long fileLength = getMediatorParameter(messageContext, AzureConstants.FILE_LENGTH, Long.class, true);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);
        String leaseAction = getMediatorParameter(messageContext, AzureConstants.LEASE_ACTION, String.class, true);
        Integer leaseDuration =
                getMediatorParameter(messageContext, AzureConstants.LEASE_DURATION, Integer.class, true);
        Boolean uncommittedDataRetained =
                getMediatorParameter(messageContext, AzureConstants.UNCOMMITTED_DATA_RETAINED, Boolean.class, true);

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        LeaseAction leaseActionConstant = getLeaseAction(leaseAction);

        try {
            AzureStorageConnectionHandler
                    azureStorageConnectionHandler =
                    (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME,
                            connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            DataLakeFileSystemClient dataLakeFileSystemClient =
                    dataLakeServiceClient.getFileSystemClient(fileSystemName);
            DataLakeFileClient dataLakeFileClient = dataLakeFileSystemClient.getFileClient(filePathToFlush);
            long fileSizeBefore = dataLakeFileClient.getProperties().getFileSize();

            Response<?> response = dataLakeFileClient.flushWithResponse(
                    fileSizeBefore + fileLength,
                    new DataLakeFileFlushOptions().setUncommittedDataRetained(uncommittedDataRetained)
                            .setRequestConditions(
                                    getRequestConditions(leaseId, ifMatch, ifNoneMatch, ifModifiedSince,
                                            ifUnmodifiedSince)
                                                 ).setPathHttpHeaders(

                                    new PathHttpHeaders().setCacheControl(cacheControl)
                                            .setContentDisposition(contentDisposition).setContentEncoding(contentEncoding)
                                            .setContentLanguage(contentLanguage).setContentType(contentType)
                                                                     ).setClose(true)
                            .setLeaseAction(leaseActionConstant).setLeaseDuration(leaseDuration)
                            .setProposedLeaseId(proposedLeaseId),
                    timeout != null ? Duration.ofSeconds(timeout.longValue()) : null,
                    null);

            if (response != null && response.getStatusCode() == 200) {
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, true, null, null);
            } else {
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, false, null, null);
            }

        } catch (DataLakeStorageException e) {
            handleConnectorException(org.wso2.carbon.connector.util.Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext,
                    e);
        } catch (ConnectException e) {
            handleConnectorException(org.wso2.carbon.connector.util.Error.CONNECTION_ERROR, messageContext, e);
        } catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }

    }

}
