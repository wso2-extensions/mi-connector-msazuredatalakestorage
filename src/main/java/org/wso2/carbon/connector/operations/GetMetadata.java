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
import org.apache.synapse.util.InlineExpressionUtil;
import org.jaxen.JaxenException;
import org.json.JSONObject;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

import java.util.Map;

/**
 * Implements the get metadata operation.
 */
public class GetMetadata extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody)
            throws JaxenException {

        String connectionName =
                getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String preprocessedFileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String preprocessedFilePath = getMediatorParameter(messageContext, AzureConstants.FILE_PATH, String.class, false);

        String fileSystemName = InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, preprocessedFileSystemName);
        String filePath = InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, preprocessedFilePath);

        try {

            DataLakeFileClient dataLakeFileClient =
                    getDataLakeFileClient(connectionName, fileSystemName, filePath);

            Map<String, String> metadata;
            metadata = dataLakeFileClient.getProperties().getMetadata();

            JSONObject responseObject = new JSONObject();
            responseObject.put(AzureConstants.STATUS, true);
            responseObject.put(AzureConstants.RESULT, metadata);

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
