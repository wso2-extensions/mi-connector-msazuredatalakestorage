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
import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.LeaseAction;
import com.azure.storage.file.datalake.options.DataLakeFileAppendOptions;
import org.apache.synapse.MessageContext;
import org.apache.synapse.util.InlineExpressionUtil;
import org.jaxen.JaxenException;
import org.json.JSONObject;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Appends data to a file in Azure Data Lake Storage.
 */
public class AppendFile extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody)
            throws JaxenException {

        String connectionName = getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String preprocessedFileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String preprocessedFilePathToAppend =
                getMediatorParameter(messageContext, AzureConstants.FILE_PATH_TO_APPEND, String.class, false);
        String inputType = getMediatorParameter(messageContext, AzureConstants.INPUT_TYPE, String.class, false);
        String preprocessedLocalFilePath =
                getMediatorParameter(messageContext, AzureConstants.LOCAL_FILE_PATH, String.class,
                        !((AzureConstants.L_LOCAL_FILE_PATH).equals(inputType)));
        String preprocessedTextContent = getMediatorParameter(messageContext, AzureConstants.TEXT_CONTENT, String.class,
                !((AzureConstants.L_TEXT_CONTENT).equals(inputType)));
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);
        Boolean flush = getMediatorParameter(messageContext, AzureConstants.FLUSH, Boolean.class, true);
        String leaseId = getMediatorParameter(messageContext, AzureConstants.LEASE_ID, String.class, true);
        String leaseAction = getMediatorParameter(messageContext, AzureConstants.LEASE_ACTION, String.class, true);
        Integer leaseDuration =
                getMediatorParameter(messageContext, AzureConstants.LEASE_DURATION, Integer.class, true);
        String proposedLeaseId =
                getMediatorParameter(messageContext, AzureConstants.PROPOSED_LEASE_ID, String.class, true);

        String fileSystemName =
                InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, preprocessedFileSystemName);
        String filePathToAppend =
                InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext,
                        preprocessedFilePathToAppend);
        String localFilePath =
                InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, preprocessedLocalFilePath);
        String textContent =
                InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, preprocessedTextContent);

        LeaseAction leaseActionConstant = getLeaseAction(leaseAction);
        long appendSize = 0;
        Response<?> response = null;

        try {

            DataLakeFileClient dataLakeFileClient =
                    getDataLakeFileClient(connectionName, fileSystemName, filePathToAppend);
            long fileSize = dataLakeFileClient.getProperties().getFileSize();
            if (textContent != null && localFilePath == null) {
                response = dataLakeFileClient.appendWithResponse(BinaryData.fromString(textContent), fileSize,
                        new DataLakeFileAppendOptions().setFlush(flush)
                                .setContentHash(MessageDigest.getInstance("MD5").digest(textContent.getBytes()))
                                .setLeaseId(leaseId).setLeaseAction(leaseActionConstant)
                                .setLeaseDuration(leaseDuration),
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null, null);
                appendSize = textContent.length();
            } else if (localFilePath != null && textContent == null) {
                Path filePath = Paths.get(localFilePath);
                byte[] fileContent = Files.readAllBytes(filePath);
                response = dataLakeFileClient.appendWithResponse(BinaryData.fromFile(filePath), fileSize,
                        new DataLakeFileAppendOptions().setFlush(flush)
                                .setContentHash(MessageDigest.getInstance("MD5").digest(fileContent))
                                .setLeaseId(leaseId).setLeaseAction(leaseActionConstant).setLeaseDuration(leaseDuration)
                                .setProposedLeaseId(proposedLeaseId),
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null, null);
                appendSize = Files.size(filePath);
            }

            Map<String, Object> attributes = new HashMap<>();

            attributes.put(AzureConstants.APPENDSIZE, appendSize);

            if (response != null && response.getStatusCode() == 202) {
                JSONObject responseObject = new JSONObject();
                responseObject.put(AzureConstants.STATUS, true);
                responseObject.put(AzureConstants.MESSAGE, "Successfully appended");
                responseObject.put(AzureConstants.APPENDSIZE, appendSize);
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, responseObject, null,
                        attributes);
            }

            // No 'else' block is needed because if the append operation fails,
            // the SDK throws an exception. We only handle the success case explicitly
            // (status code 202) and let exceptions propagate for error handling.

        } catch (DataLakeStorageException e) {
            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);
        } catch (ConnectException e) {
            handleConnectorException(Error.CONNECTION_ERROR, messageContext, e);
        } catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }

    }

}
