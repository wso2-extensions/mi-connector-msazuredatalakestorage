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

import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListFileSystemsOptions;

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
 * Implements the list of file systems operation.
 */
public class ListFileSystems extends AbstractAzureMediator {
    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String prefix = getMediatorParameter(messageContext, AzureConstants.PREFIX, String.class, true);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        List<String> fileSystems = new ArrayList<>();

        try {
            AzureStorageConnectionHandler azureStorageConnectionHandler = (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME, connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            dataLakeServiceClient.listFileSystems(
                    new ListFileSystemsOptions().setPrefix(prefix) , timeout != null ? Duration.ofSeconds(timeout.longValue()) : null)
            .forEach(fileSystem -> fileSystems.add(fileSystem.getName()));
        } catch (ConnectException e) {
            handleConnectorException(Error.CONNECTION_ERROR, messageContext, e);
        } catch (DataLakeStorageException e) {
            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);
        } catch (RuntimeException e) {
            handleConnectorException(Error.TIMEOUT_ERROR, messageContext, e);
        }
        handleConnectorResponse(messageContext, responseVariable, overwriteBody, fileSystems, null, null);
    }
}
