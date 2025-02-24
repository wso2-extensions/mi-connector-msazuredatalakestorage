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

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;

import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;

import org.wso2.carbon.connector.util.*;
import org.wso2.carbon.connector.util.Error;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements the list paths of file system operation.
 */
public class ListPaths extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String fileSystemName = getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        Boolean recursive = getMediatorParameter(messageContext, AzureConstants.RECURSIVE, Boolean.class, false);
        String path = getMediatorParameter(messageContext, AzureConstants.PATH, String.class, true);
        Integer maxResults = getMediatorParameter(messageContext, AzureConstants.MAX_RESULTS, Integer.class, true);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        try {
            AzureStorageConnectionHandler azureStorageConnectionHandler = (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME, connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient(fileSystemName);

            List<String> listPaths = new ArrayList<>();
            dataLakeFileSystemClient.listPaths(
                   new ListPathsOptions().setPath(path).setRecursive(recursive).setMaxResults(maxResults),
                   timeout != null ? Duration.ofSeconds(timeout) : null)
                    .forEach(pathItem -> listPaths.add(pathItem.getName()));

            handleConnectorResponse(messageContext, responseVariable, overwriteBody, listPaths, null, null);

        } catch (ConnectException e) {
            handleConnectorException(Error.CONNECTION_ERROR, messageContext, e);
        } catch (DataLakeStorageException e) {
            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);
        }catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }
        }

}
