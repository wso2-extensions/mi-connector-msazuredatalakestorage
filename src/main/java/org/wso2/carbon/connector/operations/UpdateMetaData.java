package org.wso2.carbon.connector.operations;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.axiom.om.OMElement;
import org.apache.commons.lang.StringUtils;
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
import java.util.HashMap;
import java.util.Map;

public class UpdateMetaData extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext){

        Object fileSystemName = messageContext.getProperty(AzureConstants.FILE_SYSTEM_NAME);
        Object filePathToAddMetaData = messageContext.getProperty(AzureConstants.FILE_PATH_TO_ADD_META_DATA);
        Object metadata = messageContext.getProperty(AzureConstants.METADATA);

        if (fileSystemName == null || filePathToAddMetaData == null || metadata == null) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.MISSING_PARAMETERS, "Mandatory parameters [fileSystemName] or [filePathToAddMetaData] or [metaData] cannot be empty.");
            handleException("Mandatory parameters [fileSystemName] or [filePathToAddMetaData] or [metaData] cannot be empty.", messageContext);
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
            DataLakeFileClient dataLakeFileClient = dataLakeFileSystemClient.getFileClient(filePathToAddMetaData.toString());

            if (dataLakeFileClient.exists() ) {


                HashMap<String, String> metadataMap = new HashMap<>();

                Gson gson = new Gson();
                Map<String, String> map = gson.fromJson(metadata.toString().replace("'",""), Map.class);

                for (Map.Entry<String, String> entry : map.entrySet()) {
                    if (StringUtils.isNotEmpty(entry.getValue())) {
                        metadataMap.put(entry.getKey(), entry.getValue());
                    }
                }
                dataLakeFileClient.setMetadata(metadataMap);
                status = true;
            }

        }catch (JsonSyntaxException e){
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.INVALID_JSON, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        }
        catch (InvalidConfigurationException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.INVALID_CONFIGURATION, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        }catch (DataLakeStorageException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.DATA_LAKE_STORAGE_GEN2_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        }  catch (ConnectException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.CONNECTION_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        } catch (Exception e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.GENERAL_ERROR, e.getMessage());
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
