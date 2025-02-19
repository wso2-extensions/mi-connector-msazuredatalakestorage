package org.wso2.carbon.connector.operations;

import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.google.gson.Gson;
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
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

public class UploadFile  extends AbstractConnector {

    @Override
    public void  connect(MessageContext messageContext){

        Object fileSystemName = messageContext.getProperty(AzureConstants.FILE_SYSTEM_NAME);
        Object filePathToUpload = messageContext.getProperty(AzureConstants.FILE_PATH_TO_UPLOAD);
        Object localFilePath = messageContext.getProperty(AzureConstants.LOCAL_FILE_PATH);
        Object finalContentType = messageContext.getProperty(AzureConstants.FINAL_CONTENT_TYPE);
        Object textContent = messageContext.getProperty(AzureConstants.TEXT_CONTENT);
        Object metadata = messageContext.getProperty(AzureConstants.METADATA);

        if (fileSystemName == null || ((filePathToUpload == null )&& (localFilePath == null)) || ((filePathToUpload == null) && (textContent == null))){
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.MISSING_PARAMETERS , "Mandatory parameters [fileSystemName], [filePathToUpload] or [localFilePath] and [textContent] cannot be empty.");
            handleException("Mandatory parameters [fileSystemName], [filePathToUpload] or [localFilePath] and [textContent] cannot be empty.", messageContext);
        }

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        Boolean status = false;

        try{
            String connectionName = AzureUtil.getConnectionName(messageContext);
            AzureStorageConnectionHandler azureStorageConnectionHandler = (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME, connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient(fileSystemName.toString());

            if (dataLakeFileSystemClient.exists()) {
                DataLakeFileClient dataLakeFileClient = dataLakeFileSystemClient.getFileClient(filePathToUpload.toString());
                if (dataLakeFileClient.exists()) {
                    AzureUtil.setErrorPropertiesToMessage(messageContext, Error.FILE_ALREADY_EXISTS_ERROR, "File already exists in the given path.");
                    handleException("File already exists in the given path.", messageContext);
                }

                if (localFilePath != null) {
                    dataLakeFileClient.uploadFromFile(localFilePath.toString());
                } else{
                    dataLakeFileClient.upload(BinaryData.fromString(textContent.toString()));
                }

                if (finalContentType != null) {
                    dataLakeFileClient.setHttpHeaders(new PathHttpHeaders().setContentType(finalContentType.toString()));
                }

                if (metadata != null && !"".equals(metadata)) {

                    HashMap<String, String> metadataMap = new HashMap<>();

                    Gson gson = new Gson();
                    Map<String, String> map = gson.fromJson(metadata.toString().replace("'",""), Map.class);

                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        if (StringUtils.isNotEmpty(entry.getValue())) {
                            metadataMap.put(entry.getKey(), entry.getValue());
                        }
                    }
                    dataLakeFileClient.setMetadata(metadataMap);


                }

                status = true;



            }else {
                AzureUtil.setErrorPropertiesToMessage(messageContext, Error.FILE_SYSTEM_DOES_NOT_EXIST, "File system does not exist.");
                handleException("File system does not exist.", messageContext);
            }

        }catch (UncheckedIOException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.FILE_IO_ERROR, e.getMessage());
            handleException(AzureConstants.ERROR_LOG_PREFIX + e.getMessage(), messageContext);
        }
        catch (InvalidConfigurationException e){
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

    private void  generateResults(MessageContext messageContext , boolean status) {
        String response = AzureUtil.generateResultPayload(
                status, !status ? AzureConstants.ERR_FILE_DOES_NOT_UPLOAD : "");
        OMElement element = null;

        try {
            element = ResultPayloadCreator.performSearchMessages(response);
        } catch (XMLStreamException e) {
            handleException("Unable to build the message.", e, messageContext);
        }

        ResultPayloadCreator.preparePayload(messageContext, element);

    }
}
