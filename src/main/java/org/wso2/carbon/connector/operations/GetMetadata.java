/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.connector.operations;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import org.apache.synapse.MessageContext;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.json.JSONObject;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

import java.util.Map;

/**
 * Implements the get metadata operation.
 */
public class GetMetadata extends AbstractAzureMediator {

    /**
     * Retrieves the metadata of a file in Azure Data Lake Storage.
     *
     * @param azureStorageConnectionHandler Azure Storage connection handler
     * @param fileSystemName                Name of the file system
     * @param filePath                      Path of the file
     * @return Metadata of the file
     * @throws ConnectException If an error occurs while retrieving the metadata
     * @throws JSONException    If an error occurs while creating the JSON object
     */
    @NotNull
    private static JSONObject getJsonObject(AzureStorageConnectionHandler azureStorageConnectionHandler,
                                            String fileSystemName, String filePath)
            throws ConnectException, JSONException {

        DataLakeFileClient dataLakeFileClient =
                azureStorageConnectionHandler.getDataLakeServiceClient().getFileSystemClient(fileSystemName)
                        .getFileClient(filePath);

        Map<String, String> metadata;
        metadata = dataLakeFileClient.getProperties().getMetadata();

        JSONObject responseObject = new JSONObject();
        responseObject.put("success", true);
        responseObject.put("metadata", metadata);
        return responseObject;
    }

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {

        String connectionName =
                getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String fileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String filePath = getMediatorParameter(messageContext, AzureConstants.FILE_PATH, String.class, false);

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        try {
            AzureStorageConnectionHandler azureStorageConnectionHandler =
                    (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME,
                            connectionName);
            JSONObject responseObject = getJsonObject(azureStorageConnectionHandler, fileSystemName, filePath);

            handleConnectorResponse(messageContext, responseVariable, overwriteBody, responseObject, null,
                    null);
        } catch (ConnectException e) {
            handleConnectorException(Error.CONNECTION_ERROR, messageContext, e);
        } catch (DataLakeStorageException e) {
            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);
        } catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }
    }

}
