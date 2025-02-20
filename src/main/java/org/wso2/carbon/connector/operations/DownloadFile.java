package org.wso2.carbon.connector.operations;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
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

public class DownloadFile extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext){
        Object fileSystemName = messageContext.getProperty(AzureConstants.FILE_SYSTEM_NAME);
        Object filePathToDownload = messageContext.getProperty(AzureConstants.FILE_PATH_TO_DOWNLOAD);
        Object downloadLocation = messageContext.getProperty(AzureConstants.DOWNLOAD_LOCATION);

        if (fileSystemName == null || filePathToDownload == null || downloadLocation == null) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.MISSING_PARAMETERS, "Mandatory parameters [fileSystemName] or [filePathToDownload] or [downloadLocation] cannot be empty.");
            handleException("Mandatory parameters [fileSystemName] or [filePathToDownload] or [downloadLocation] cannot be empty.", messageContext);
        }

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        boolean status = false;

        try{
            String connectionName = AzureUtil.getConnectionName(messageContext);
            AzureStorageConnectionHandler azureStorageConnectionHandler = (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME, connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient(fileSystemName.toString());
            DataLakeFileClient dataLakeFileClient = dataLakeFileSystemClient.getFileClient(filePathToDownload.toString());

            if (dataLakeFileClient.exists()) {
                dataLakeFileClient.readToFile(downloadLocation.toString());
                status = true;
            }
        } catch (InvalidConfigurationException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.INVALID_CONFIGURATION, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        } catch (ConnectException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.CONNECTION_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        } catch (Exception e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.GENERAL_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        }


        generalResult(messageContext, status);
    }

    private void generalResult(MessageContext messageContext, boolean status){
        String response = AzureUtil.generateResultPayload(status, !status ? AzureConstants.ERR_FILE_DOES_NOT_EXIST : "");
        OMElement element = null;

        try{
            element = ResultPayloadCreator.performSearchMessages(response);
        } catch (XMLStreamException e) {
            handleException("Error occurred while processing the response message", e, messageContext);
        }

        if (element != null) {
            messageContext.getEnvelope().getBody().getFirstElement().detach();
            messageContext.getEnvelope().getBody().addChild(element);
        }
    }
}
