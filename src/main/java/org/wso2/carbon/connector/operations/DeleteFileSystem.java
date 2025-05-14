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

import com.azure.core.http.rest.Response;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.options.DataLakePathDeleteOptions;
import org.apache.synapse.MessageContext;
import org.apache.synapse.util.InlineExpressionUtil;
import org.jaxen.JaxenException;
import org.json.JSONObject;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

import java.time.Duration;

/**
 * Deletes a file system in Azure Data Lake.
 */
public class DeleteFileSystem extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody)
            throws JaxenException {

        String connectionName =
                getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String preprocessedFileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String ifMatch = getMediatorParameter(messageContext, AzureConstants.IF_MATCH, String.class, true);
        String ifNoneMatch = getMediatorParameter(messageContext, AzureConstants.IF_NONE_MATCH, String.class, true);
        String ifModifiedSince = getMediatorParameter(messageContext, AzureConstants.IF_MODIFIED_SINCE, String.class,
                true);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);
        String leaseId = getMediatorParameter(messageContext, AzureConstants.LEASE_ID, String.class, true);
        String ifUnmodifiedSince = getMediatorParameter(messageContext, AzureConstants.IF_UNMODIFIED_SINCE,
                String.class, true);

        String fileSystemName =
                InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, preprocessedFileSystemName);

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        try {
            AzureStorageConnectionHandler azureStorageConnectionHandler =
                    (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME,
                            connectionName);
            DataLakeFileSystemClient dataLakeFileSystemClient =
                    azureStorageConnectionHandler.getDataLakeServiceClient().getFileSystemClient(fileSystemName);
            DataLakeRequestConditions requestConditions = getRequestConditions(leaseId, ifMatch,
                    ifModifiedSince, ifNoneMatch, ifUnmodifiedSince);
            Response<?> response = dataLakeFileSystemClient.deleteIfExistsWithResponse(
                    new DataLakePathDeleteOptions().setRequestConditions(requestConditions).setIsRecursive(true),
                    timeout != null ? Duration.ofSeconds(timeout.longValue()) : null, null);

            if (response.getStatusCode() == 202) {
                JSONObject responseObject = new JSONObject();
                responseObject.put(AzureConstants.STATUS, true);
                responseObject.put(AzureConstants.MESSAGE, "Successfully deleted the file system");
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, responseObject, null,
                        null);
            }

            // No 'else' block is needed because if the delete file system operation fails,
            // the SDK throws an exception. We only handle the success case explicitly
            // (status code 202) and let exceptions propagate for error handling.

        } catch (ConnectException e) {
            handleConnectorException(Error.CONNECTION_ERROR, messageContext, e);
        } catch (DataLakeStorageException e) {
            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);
        } catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }

    }
}
