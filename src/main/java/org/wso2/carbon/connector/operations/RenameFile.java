package org.wso2.carbon.connector.operations;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import org.apache.axiom.om.OMElement;
import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.exceptions.InvalidConfigurationException;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.AzureUtil;
import org.wso2.carbon.connector.util.ResultPayloadCreator;

import javax.xml.stream.XMLStreamException;

public class RenameFile extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext) {

        Object fileSystemName = messageContext.getProperty(AzureConstants.FILE_SYSTEM_NAME);
        Object filePathToRename = messageContext.getProperty(AzureConstants.FILE_PATH_TO_RENAME);
        Object newFilePath = messageContext.getProperty(AzureConstants.NEW_FILE_PATH);
        Object newFileSystemName = messageContext.getProperty(AzureConstants.NEW_FILE_SYSTEM_Name);

        if (fileSystemName == null || filePathToRename == null || newFilePath == null || newFileSystemName == null) {
            handleException("Mandatory parameters [fileSystemName] or [filePathToRename] or [newFilePath] or [newFileSystem] cannot be empty.", messageContext);
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
            DataLakeFileClient dataLakeFileClient = dataLakeFileSystemClient.getFileClient(filePathToRename.toString());

            if (dataLakeFileClient.exists()) {
                dataLakeFileClient.rename(newFileSystemName.toString(), newFilePath.toString());
                status = true;
            }

        } catch (InvalidConfigurationException e) {
            // Set error properties to message context
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        } catch (ConnectException e) {
            // Set error properties to message context
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        } catch (Exception e) {
            // Set error properties to message context
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        }

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
