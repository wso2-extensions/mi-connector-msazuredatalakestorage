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
import com.azure.storage.common.ParallelTransferOptions;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.DownloadRetryOptions;
import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.options.ReadToFileOptions;
import org.apache.synapse.MessageContext;
import org.apache.synapse.util.InlineExpressionUtil;
import org.jaxen.JaxenException;
import org.json.JSONObject;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

import java.io.UncheckedIOException;
import java.time.Duration;

/**
 * Downloads a file from Azure Data Lake.
 */
public class DownloadFile extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody)
            throws JaxenException {

        String connectionName =
                getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String preprocessedFileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String preprocessedFilePath =
                getMediatorParameter(messageContext, AzureConstants.FILE_PATH_TO_DOWNLOAD, String.class,
                        false);
        String ifMatch = getMediatorParameter(messageContext, AzureConstants.IF_MATCH, String.class, true);
        String ifNoneMatch = getMediatorParameter(messageContext, AzureConstants.IF_NONE_MATCH, String.class, true);
        String ifModifiedSince = getMediatorParameter(messageContext, AzureConstants.IF_MODIFIED_SINCE,
                String.class, true);
        Integer blockSize = getMediatorParameter(messageContext, AzureConstants.BLOCK_SIZE, Integer.class, true);
        Integer maxConcurrency = getMediatorParameter(messageContext, AzureConstants.MAX_CONCURRENCY, Integer.class,
                true);
        Integer maxRetryRequests =
                getMediatorParameter(messageContext, AzureConstants.MAX_RETRY_REQUESTS, Integer.class,
                        true);
        String preprocessedDownloadFilePath =
                getMediatorParameter(messageContext, AzureConstants.DOWNLOAD_LOCATION, String.class, false);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);
        String leaseId = getMediatorParameter(messageContext, AzureConstants.LEASE_ID, String.class, true);
        String ifUnmodifiedSince = getMediatorParameter(messageContext, AzureConstants.IF_UNMODIFIED_SINCE,
                String.class, true);
        Integer offset = getMediatorParameter(messageContext, AzureConstants.OFFSET, Integer.class, true);
        Integer count = getMediatorParameter(messageContext, AzureConstants.COUNT, Integer.class, true);
        Boolean rangeGetContentMd5 = getMediatorParameter(messageContext, AzureConstants.RANGE_GET_CONTENT_MD5,
                Boolean.class, true);

        String fileSystemName =
                InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, preprocessedFileSystemName);
        String filePath =
                InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, preprocessedFilePath);
        String downloadFilePath = InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext,
                preprocessedDownloadFilePath);

        Long blockSizeL = blockSize != null ? blockSize.longValue() * 1024L * 1024L : null;

        try {

            DataLakeFileClient dataLakeFileClient = getDataLakeFileClient(connectionName, fileSystemName, filePath);
            ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
                    .setBlockSizeLong(blockSizeL)
                    .setMaxConcurrency(maxConcurrency);

            DataLakeRequestConditions requestConditions = getRequestConditions(leaseId, ifMatch,
                    ifModifiedSince, ifNoneMatch, ifUnmodifiedSince);

            FileRange fileRange;
            if (offset == null) {
                fileRange = null;
            } else if (count == null) {
                fileRange = new FileRange(offset.longValue());
            } else {
                fileRange = new FileRange(offset.longValue(), count.longValue());
            }

            Response<?> response;

            if (maxRetryRequests == null) {
                response = dataLakeFileClient.readToFileWithResponse(
                        new ReadToFileOptions(downloadFilePath).setRangeGetContentMd5(rangeGetContentMd5)
                                .setRange(fileRange).setParallelTransferOptions(parallelTransferOptions)
                                .setDataLakeRequestConditions(requestConditions),
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null, null)
                ;
            } else {
                response = dataLakeFileClient.readToFileWithResponse(
                        new ReadToFileOptions(downloadFilePath).setRangeGetContentMd5(rangeGetContentMd5)
                                .setRange(fileRange).setParallelTransferOptions(parallelTransferOptions)
                                .setDownloadRetryOptions(
                                        new DownloadRetryOptions().setMaxRetryRequests(maxRetryRequests))
                                .setDataLakeRequestConditions(requestConditions),
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null, null);
            }

            if (response.getStatusCode() == 206) {
                JSONObject responseObject = new JSONObject();
                responseObject.put(AzureConstants.STATUS, true);
                responseObject.put(AzureConstants.MESSAGE, "Successfully downloaded the file");
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, responseObject, null, null);
            }

            // No 'else' block is needed because if the download file operation fails,
            // the SDK throws an exception. We only handle the success case explicitly
            // (status code 206) and let exceptions propagate for error handling.

        } catch (ConnectException e) {
            handleConnectorException(Error.CONNECTION_ERROR, messageContext, e);
        } catch (DataLakeStorageException e) {
            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);
        } catch (UncheckedIOException e) {
            handleConnectorException(Error.FILE_IO_ERROR, messageContext, e);
        } catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }

    }

}
