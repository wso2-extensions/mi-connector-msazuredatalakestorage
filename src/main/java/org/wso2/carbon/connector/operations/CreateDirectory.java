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
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.options.DataLakePathCreateOptions;
import org.apache.synapse.MessageContext;
import org.json.JSONObject;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;
import org.wso2.carbon.connector.util.Utils;
import org.apache.synapse.util.InlineExpressionUtil;

import java.time.Duration;
import java.util.HashMap;

/**
 * Implements the create directory operation.
 */
public class CreateDirectory extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {

        String connectionName =
                getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String fileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String directoryName =
                getMediatorParameter(messageContext, AzureConstants.DIRECTORY_NAME, String.class, false);
        String metadata =
                getMediatorParameter(messageContext, AzureConstants.METADATA, String.class, true);

        Integer timeout =
                getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);
        String contentLanguage =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_LANGUAGE, String.class, true);
        String contentType =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_TYPE, String.class, true);
        String cacheControl =
                getMediatorParameter(messageContext, AzureConstants.CACHE_CONTROL, String.class, true);
        String contentDisposition =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_DISPOSITION, String.class, true);
        String contentEncoding =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_ENCODING, String.class, true);
        String permissions =
                getMediatorParameter(messageContext, AzureConstants.PERMISSIONS, String.class, true);
        String umask =
                getMediatorParameter(messageContext, AzureConstants.UMASK, String.class, true);
        String owner =
                getMediatorParameter(messageContext, AzureConstants.OWNER, String.class, true);
        String group =
                getMediatorParameter(messageContext, AzureConstants.GROUP, String.class, true);
        String sourceLeaseId =
                getMediatorParameter(messageContext, AzureConstants.SOURCE_LEASE_ID, String.class, true);

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        try {
            AzureStorageConnectionHandler azureStorageConnectionHandler =
                    (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME,
                            connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            DataLakeFileSystemClient dataLakeFileSystemClient =
                    dataLakeServiceClient.getFileSystemClient(fileSystemName);
            DataLakeDirectoryClient dataLakeDirectoryClient =
                    dataLakeFileSystemClient.getDirectoryClient(directoryName);

            metadata = InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, metadata);

            PathHttpHeaders headers = new PathHttpHeaders()
                    .setCacheControl(cacheControl)
                    .setContentType(contentType)
                    .setContentDisposition(contentDisposition)
                    .setContentEncoding(contentEncoding)
                    .setContentLanguage(contentLanguage);

            HashMap<String, String> metadataMap = new HashMap<>();

            if (metadata != null && !metadata.isEmpty()) {
                Utils.addDataToMapFromJsonString(metadata, metadataMap);
            }

            Response<?> response = dataLakeDirectoryClient.createIfNotExistsWithResponse(

                    new DataLakePathCreateOptions().setGroup(group).setOwner(owner).setUmask(umask)
                            .setSourceLeaseId(sourceLeaseId).setMetadata(metadataMap).setPermissions(permissions)
                            .setPathHttpHeaders(headers),
                    timeout != null ? Duration.ofSeconds(timeout.longValue()) : null, null);

            if (response.getStatusCode() == 201) {
                JSONObject responseObject = new JSONObject();
                responseObject.put("success", true);
                responseObject.put("message", "Successfully created the directory");
                responseObject.put("directoryName", directoryName);
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, responseObject, null, null);
            } else {
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, false, null, null);
            }

        } catch (ConnectException e) {
            handleConnectorException(Error.CONNECTION_ERROR, messageContext, e);
        } catch (DataLakeStorageException e) {
            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);
        } catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }

    }

}
