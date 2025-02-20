package org.wso2.carbon.connector.operations;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
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

public class ListPathsOfFileSystem extends AbstractConnector {

    @Override
    public void connect (MessageContext messageContext){
        Object fileSystemName = messageContext.getProperty(AzureConstants.FILE_SYSTEM_NAME);

        if (fileSystemName == null){
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.MISSING_PARAMETERS , "Mandatory parameter [fileSystemName] cannot be empty.");
            handleException("Mandatory parameter [fileSystemName] cannot be empty.", messageContext);
        }

        OMFactory factory = OMAbstractFactory.getOMFactory();
        OMNamespace ns = factory.createOMNamespace(AzureConstants.AZURE_NAMESPACE, AzureConstants.NAMESPACE);
        OMElement result = factory.createOMElement(AzureConstants.RESULT, ns);
        ResultPayloadCreator.preparePayload(messageContext, result);
        OMElement pathsElement = factory.createOMElement(AzureConstants.PATHS, ns);
        result.addChild(pathsElement);

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();


        try{
            String connectionName = AzureUtil.getConnectionName(messageContext);
            AzureStorageConnectionHandler azureStorageConnectionHandler = (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME, connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient(fileSystemName.toString());

            if (dataLakeFileSystemClient.exists()){
                dataLakeFileSystemClient.listPaths().forEach(pathItem -> {
                    OMElement messageElement = factory.createOMElement(AzureConstants.PATH, ns);
                    messageElement.setText(pathItem.getName());
                    pathsElement.addChild(messageElement);
                });
            } else {
                generateResults(messageContext , AzureConstants.ERR_FILE_SYSTEM_DOES_NOT_EXIST);
            }

        } catch (InvalidConfigurationException e){
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.INVALID_CONFIGURATION, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        }catch (DataLakeStorageException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.DATA_LAKE_STORAGE_GEN2_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        } catch (ConnectException e){
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.CONNECTION_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        } catch (Exception e){
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.GENERAL_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        }


        messageContext.getEnvelope().getBody().addChild(result);






    }

    private void generateResults(MessageContext messageContext, String status) {
        String response = AzureUtil.generateResultPayload(false, status);
        OMElement element = null;
        try {
            element = ResultPayloadCreator.performSearchMessages(response);
        } catch (XMLStreamException e) {
            handleException("Unable to build the message.", e, messageContext);
        }
        ResultPayloadCreator.preparePayload(messageContext, element);
    }
}
