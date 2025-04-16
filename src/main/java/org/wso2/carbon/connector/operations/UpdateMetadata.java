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
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.google.gson.JsonSyntaxException;
import org.apache.synapse.MessageContext;
import org.apache.synapse.util.InlineExpressionUtil;
import org.json.JSONObject;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;
import org.wso2.carbon.connector.util.Utils;

import java.time.Duration;
import java.util.HashMap;

/**
 * Updates metadata for a file in Azure Data Lake Storage.
 */
public class UpdateMetadata extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {

        String connectionName = getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String fileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String filePathToAddMetaData =
                getMediatorParameter(messageContext, AzureConstants.FILE_PATH_TO_ADD_META_DATA, String.class, false);
        String ifMatch = getMediatorParameter(messageContext, AzureConstants.IF_MATCH, String.class, true);
        String ifModifiedSince =
                getMediatorParameter(messageContext, AzureConstants.IF_MODIFIED_SINCE, String.class, true);
        String ifNoneMatch = getMediatorParameter(messageContext, AzureConstants.IF_NONE_MATCH, String.class, true);
        String metadata = getMediatorParameter(messageContext, AzureConstants.METADATA, String.class, false);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);
        String leaseId = getMediatorParameter(messageContext, AzureConstants.LEASE_ID, String.class, true);
        String ifUnmodifiedSince =
                getMediatorParameter(messageContext, AzureConstants.IF_UNMODIFIED_SINCE, String.class, true);

        try {
            DataLakeFileClient dataLakeFileClient =
                    getDataLakeFileClient(connectionName, fileSystemName, filePathToAddMetaData);

            DataLakeRequestConditions requestConditions = getRequestConditions(leaseId, ifMatch, ifModifiedSince,
                    ifNoneMatch, ifUnmodifiedSince);
            metadata = InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, metadata);

            HashMap<String, String> metadataMap = new HashMap<>();

            String metadataString = metadata != null ? metadata : "";

            Utils.addDataToMapFromArrayString(metadataString, metadataMap);

            Response<?> response = dataLakeFileClient.setMetadataWithResponse(
                    metadataMap,
                    requestConditions,
                    timeout != null ? Duration.ofSeconds(timeout.longValue()) : null,
                    null);

            if (response.getStatusCode() == 200) {
                JSONObject responseObject = new JSONObject();
                responseObject.put(AzureConstants.STATUS, true);
                responseObject.put(AzureConstants.MESSAGE, "Successfully updated the metadata");
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, responseObject, null, null);
            }

            // No 'else' block is needed because if the update metadata operation fails,
            // the SDK throws an exception. We only handle the success case explicitly
            // (status code 200), and let exceptions propagate for error handling.

        } catch (DataLakeStorageException e) {

            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);

        } catch (ConnectException e) {

            handleConnectorException(Error.CONNECTION_ERROR, messageContext, e);

        } catch (JsonSyntaxException e) {

            handleConnectorException(Error.INVALID_JSON, messageContext, e);

        } catch (Exception e) {

            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);

        }
    }

}
