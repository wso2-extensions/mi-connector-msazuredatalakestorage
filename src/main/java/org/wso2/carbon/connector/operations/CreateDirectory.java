package org.wso2.carbon.connector.operations;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
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
import org.wso2.carbon.connector.util.Error;
import org.wso2.carbon.connector.util.ResultPayloadCreator;

import javax.xml.stream.XMLStreamException;

public class CreateDirectory extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext){
        Object fileSystemName = messageContext.getProperty(AzureConstants.FILE_SYSTEM_NAME);
        Object directoryName = messageContext.getProperty(AzureConstants.DIRECTORY_NAME);



        if (fileSystemName == null || directoryName == null){
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.MISSING_PARAMETERS , "Mandatory parameter [fileSystemName] or [directoryName] cannot be empty.");
            handleException("Mandatory parameter [fileSystemName] or [directoryName] cannot be empty.", messageContext);
        }

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        boolean status = false;

        try{
            String connectionName = AzureUtil.getConnectionName(messageContext);
            AzureStorageConnectionHandler azureStorageConnectionHandler = (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME, connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient(fileSystemName.toString());
            DataLakeDirectoryClient dataLakeDirectoryClient = dataLakeFileSystemClient.getDirectoryClient(directoryName.toString());
            if (!dataLakeDirectoryClient.exists()){
                dataLakeDirectoryClient.create();
                status = true;
            }
        }catch (InvalidConfigurationException e){
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.INVALID_CONFIGURATION, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        }catch (ConnectException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.CONNECTION_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        } catch (Exception e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.GENERAL_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        }

        generateResults(messageContext, status);

    }

    private void generateResults(MessageContext messageContext , boolean status){
        String response = AzureUtil.generateResultPayload(
                status , !status ? AzureConstants.ERR_DIRECTORY_ALREADY_EXISTS : "");
        OMElement element = null;

        try {
            element = ResultPayloadCreator.performSearchMessages(response);
        } catch (XMLStreamException e) {
            handleException("Unable to build the message.", e, messageContext);
        }
        ResultPayloadCreator.preparePayload(messageContext, element);
    }
}
