package org.wso2.carbon.connector.operations;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import org.apache.axiom.om.OMElement;
import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.exceptions.InvalidConfigurationException;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.AzureUtil;
import org.wso2.carbon.connector.util.Error;
import org.wso2.carbon.connector.util.ResultPayloadCreator;

import javax.xml.stream.XMLStreamException;

public class DeleteFile extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext) {
        Object fileSystemName = messageContext.getProperty(AzureConstants.FILE_SYSTEM_NAME);
        Object filePathToDelete = messageContext.getProperty(AzureConstants.FILE_PATH_TO_DELETE);

        if (fileSystemName == null || filePathToDelete == null) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.MISSING_PARAMETERS, "Mandatory parameters [fileSystemName] or [filePathToDelete] cannot be empty.");
            handleException("Mandatory parameters [fileSystemName] or [filePathToDelete] cannot be empty.", messageContext);
        }

        // Connection handler
        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        boolean status = false;

        try {
            // Get connection name
            String connectionName = AzureUtil.getConnectionName(messageContext);
            // Get azure storage connection handler
            AzureStorageConnectionHandler azureStorageConnectionHandler = (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME, connectionName);
            // Get data lake service client
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            // Get data lake file system client
            DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient(fileSystemName.toString());
            // Get data lake file client
            DataLakeFileClient dataLakeFileClient = dataLakeFileSystemClient.getFileClient(filePathToDelete.toString());
            // Check if file exists
           status = dataLakeFileClient.deleteIfExists();

        } catch (InvalidConfigurationException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.INVALID_CONFIGURATION, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        } catch (DataLakeStorageException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.DATA_LAKE_STORAGE_GEN2_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        } catch (ConnectException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.CONNECTION_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        } catch (Exception e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.GENERAL_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        }

        // Generate results
        generateResults(messageContext, status);

    }

    private void generateResults(MessageContext messageContext, boolean status) {
        // Generate results
        String response = AzureUtil.generateResultPayload(status, !status ? AzureConstants.ERR_FILE_DOES_NOT_EXIST : "");

        OMElement element = null;

        try {
            element = ResultPayloadCreator.performSearchMessages(response);
        } catch (XMLStreamException e) {
            handleException("Unable to build the message.", e, messageContext);
        }
        ResultPayloadCreator.preparePayload(messageContext, element);

    }
}
