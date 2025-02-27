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
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.DownloadRetryOptions;
import com.azure.storage.file.datalake.models.FileRange;
import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

import java.io.ByteArrayOutputStream;
import java.time.Duration;

/**
 * Implements the read file operation.
 */
public class ReadFile extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {

        String connectionName =
                getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String fileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String filePath = getMediatorParameter(messageContext, AzureConstants.FILE_PATH, String.class, false);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);
        Integer count = getMediatorParameter(messageContext, AzureConstants.COUNT, Integer.class, true);
        Integer offset = getMediatorParameter(messageContext, AzureConstants.OFFSET, Integer.class, true);
        Integer maxRetryRequests =
                getMediatorParameter(messageContext, AzureConstants.MAX_RETRY_REQUESTS, Integer.class, true);
        String leaseId = getMediatorParameter(messageContext, AzureConstants.LEASE_ID, String.class, true);
        String ifUnmodifiedSince =
                getMediatorParameter(messageContext, AzureConstants.IF_UNMODIFIED_SINCE, String.class, true);
        String ifMatch = getMediatorParameter(messageContext, AzureConstants.IF_MATCH, String.class, true);
        String ifNoneMatch = getMediatorParameter(messageContext, AzureConstants.IF_NONE_MATCH, String.class, true);
        String ifModifiedSince =
                getMediatorParameter(messageContext, AzureConstants.IF_MODIFIED_SINCE, String.class, true);

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        try {
            AzureStorageConnectionHandler azureStorageConnectionHandler =
                    (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME,
                            connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            DataLakeFileSystemClient dataLakeFileSystemClient =
                    dataLakeServiceClient.getFileSystemClient(fileSystemName);

            FileRange fileRange;
            if (offset == null) {
                fileRange = null;
            } else if (count == null) {
                fileRange = new FileRange(offset.longValue());
            } else {
                fileRange = new FileRange(offset.longValue(), count.longValue());
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            DataLakeRequestConditions requestConditions = getRequestConditions(leaseId, ifMatch,
                    ifModifiedSince, ifNoneMatch, ifUnmodifiedSince);
            dataLakeFileSystemClient.getFileClient(filePath).readWithResponse(
                    outputStream, fileRange,
                    maxRetryRequests != null ? new DownloadRetryOptions().setMaxRetryRequests(maxRetryRequests) : null,
                    requestConditions,
                    false,
                    timeout != null ? Duration.ofSeconds(timeout) : null,
                    null
                                                                             );
            String content = outputStream.toString();

            handleConnectorResponse(messageContext, responseVariable, overwriteBody, content, null, null);
        } catch (ConnectException e) {
            handleConnectorException(Error.CONNECTION_ERROR, messageContext, e);
        } catch (DataLakeStorageException e) {
            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);
        } catch (RuntimeException e) {
            handleConnectorException(Error.TIMEOUT_ERROR, messageContext, e);
        } catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }

    }
}
