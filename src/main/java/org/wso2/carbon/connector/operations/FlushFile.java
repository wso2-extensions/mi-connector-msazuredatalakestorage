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
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.LeaseAction;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.options.DataLakeFileFlushOptions;
import org.apache.synapse.MessageContext;
import org.apache.synapse.util.InlineExpressionUtil;
import org.jaxen.JaxenException;
import org.json.JSONObject;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

import java.time.Duration;

/**
 * Flushes the specified file in Azure Data Lake.
 */
public class FlushFile extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody)
            throws JaxenException {

        String connectionName = getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String preprocessedFileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String preprocessedFilePathToFlush =
                getMediatorParameter(messageContext, AzureConstants.FILE_PATH_TO_FLUSH, String.class, false);
        String contentLanguage =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_LANGUAGE, String.class, true);
        String ifMatch = getMediatorParameter(messageContext, AzureConstants.IF_MATCH, String.class, true);
        String ifNoneMatch = getMediatorParameter(messageContext, AzureConstants.IF_NONE_MATCH, String.class, true);
        String ifModifiedSince =
                getMediatorParameter(messageContext, AzureConstants.IF_MODIFIED_SINCE, String.class, true);
        String ifUnmodifiedSince =
                getMediatorParameter(messageContext, AzureConstants.IF_UNMODIFIED_SINCE, String.class, true);
        String contentType = getMediatorParameter(messageContext, AzureConstants.CONTENT_TYPE, String.class, true);
        String contentDisposition =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_DISPOSITION, String.class, true);
        String contentEncoding =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_ENCODING, String.class, true);
        String cacheControl = getMediatorParameter(messageContext, AzureConstants.CACHE_CONTROL, String.class, true);
        String leaseId = getMediatorParameter(messageContext, AzureConstants.LEASE_ID, String.class, true);
        String proposedLeaseId =
                getMediatorParameter(messageContext, AzureConstants.PROPOSED_LEASE_ID, String.class, true);
        Integer fileLength = getMediatorParameter(messageContext, AzureConstants.FILE_LENGTH, Integer.class, true);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);
        String leaseAction = getMediatorParameter(messageContext, AzureConstants.LEASE_ACTION, String.class, true);
        Integer leaseDuration =
                getMediatorParameter(messageContext, AzureConstants.LEASE_DURATION, Integer.class, true);
        Boolean uncommittedDataRetained =
                getMediatorParameter(messageContext, AzureConstants.UNCOMMITTED_DATA_RETAINED, Boolean.class, true);

        String fileSystemName =
                InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, preprocessedFileSystemName);
        String filePathToFlush = InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext,
                preprocessedFilePathToFlush);

        LeaseAction leaseActionConstant = getLeaseAction(leaseAction);

        try {
            DataLakeFileClient dataLakeFileClient =
                    getDataLakeFileClient(connectionName, fileSystemName, filePathToFlush);
            long fileSizeBefore = dataLakeFileClient.getProperties().getFileSize();

            Response<?> response = dataLakeFileClient.flushWithResponse(
                    fileSizeBefore + fileLength.longValue(),
                    new DataLakeFileFlushOptions().setUncommittedDataRetained(uncommittedDataRetained)
                            .setRequestConditions(
                                    getRequestConditions(leaseId, ifMatch, ifNoneMatch, ifModifiedSince,
                                            ifUnmodifiedSince)
                                                 ).setPathHttpHeaders(

                                    new PathHttpHeaders().setCacheControl(cacheControl)
                                            .setContentDisposition(contentDisposition).setContentEncoding(contentEncoding)
                                            .setContentLanguage(contentLanguage).setContentType(contentType)
                                                                     ).setClose(true)
                            .setLeaseAction(leaseActionConstant).setLeaseDuration(leaseDuration)
                            .setProposedLeaseId(proposedLeaseId),
                    timeout != null ? Duration.ofSeconds(timeout.longValue()) : null,
                    null);

            if (response != null && response.getStatusCode() == 200) {
                JSONObject responseObject = new JSONObject();
                responseObject.put(AzureConstants.STATUS, true);
                responseObject.put(AzureConstants.MESSAGE, "Successfully flushed");
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, responseObject, null, null);
            }

            // No 'else' block is needed because if the flush operation fails,
            // the SDK throws an exception. We only handle the success case explicitly
            // (status code 200) and let exceptions propagate for error handling.

        } catch (DataLakeStorageException e) {
            handleConnectorException(org.wso2.carbon.connector.util.Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext,
                    e);
        } catch (ConnectException e) {
            handleConnectorException(org.wso2.carbon.connector.util.Error.CONNECTION_ERROR, messageContext, e);
        } catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }

    }

}
