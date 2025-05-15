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
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import org.apache.synapse.MessageContext;
import org.apache.synapse.util.InlineExpressionUtil;
import org.jaxen.JaxenException;
import org.json.JSONObject;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Lists the paths in a specified file system.
 */
public class ListPaths extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody)
            throws JaxenException {

        String connectionName =
                getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String preprocessedFileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        Boolean recursive =
                getMediatorParameter(messageContext, AzureConstants.RECURSIVE, Boolean.class, false);
        String preprocessedPath =
                getMediatorParameter(messageContext, AzureConstants.PATH, String.class, true);
        Integer maxResults =
                getMediatorParameter(messageContext, AzureConstants.MAX_RESULTS, Integer.class, true);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);

        String fileSystemName =
                InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, preprocessedFileSystemName);
        String path = (preprocessedPath != null) ?
                InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, preprocessedPath) : null;

        try {

            DataLakeFileSystemClient dataLakeFileSystemClient =
                    getDataLakeFileSystemClient(connectionName, fileSystemName);
            List<String> listPaths = new ArrayList<>();
            dataLakeFileSystemClient.listPaths(
                            new ListPathsOptions().setPath(path).setRecursive(recursive).setMaxResults(maxResults),
                            timeout != null ? Duration.ofSeconds(timeout) : null)
                    .forEach(pathItem -> listPaths.add(pathItem.getName()));

            JSONObject responseObject = new JSONObject();
            responseObject.put(AzureConstants.STATUS, true);
            responseObject.put(AzureConstants.RESULT, listPaths);

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
