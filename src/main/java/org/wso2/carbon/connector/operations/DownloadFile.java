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

import com.azure.storage.common.ParallelTransferOptions;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.DownloadRetryOptions;
import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.options.ReadToFileOptions;
import org.apache.synapse.MessageContext;
import org.json.JSONObject;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

import java.io.UncheckedIOException;
import java.time.Duration;

/**
 * Implements the download file operation.
 */
public class DownloadFile extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {

        String connectionName =
                getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String fileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String filePath =
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
        String downloadFilePath =
                getMediatorParameter(messageContext, AzureConstants.DOWNLOAD_LOCATION, String.class, false);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);
        String leaseId = getMediatorParameter(messageContext, AzureConstants.LEASE_ID, String.class, true);
        String ifUnmodifiedSince = getMediatorParameter(messageContext, AzureConstants.IF_UNMODIFIED_SINCE,
                String.class, true);
        Integer offset = getMediatorParameter(messageContext, AzureConstants.OFFSET, Integer.class, true);
        Integer count = getMediatorParameter(messageContext, AzureConstants.COUNT, Integer.class, true);
        Boolean rangeGetContentMd5 = getMediatorParameter(messageContext, AzureConstants.RANGE_GET_CONTENT_MD5,
                Boolean.class, true);

        Long blockSizeL = blockSize != null ? blockSize.longValue() * 1024L * 1024L : null;

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        try {
            AzureStorageConnectionHandler azureStorageConnectionHandler =
                    (AzureStorageConnectionHandler) handler.getConnection(AzureConstants.CONNECTOR_NAME,
                            connectionName);
            DataLakeServiceClient dataLakeServiceClient = azureStorageConnectionHandler.getDataLakeServiceClient();
            DataLakeFileSystemClient dataLakeFileSystemClient =
                    dataLakeServiceClient.getFileSystemClient(fileSystemName);
            DataLakeFileClient dataLakeFileClient = dataLakeFileSystemClient.getFileClient(filePath);

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

            if (maxRetryRequests == null || maxRetryRequests < 0) {
                dataLakeFileClient.readToFileWithResponse(
                        new ReadToFileOptions(downloadFilePath).setRangeGetContentMd5(rangeGetContentMd5)
                                .setRange(fileRange).setParallelTransferOptions(parallelTransferOptions)
                                .setDataLakeRequestConditions(requestConditions),
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null, null)
                ;
            } else {
                dataLakeFileClient.readToFileWithResponse(
                        new ReadToFileOptions(downloadFilePath).setRangeGetContentMd5(rangeGetContentMd5)
                                .setRange(fileRange).setParallelTransferOptions(parallelTransferOptions)
                                .setDownloadRetryOptions(
                                        new DownloadRetryOptions().setMaxRetryRequests(maxRetryRequests))
                                .setDataLakeRequestConditions(requestConditions),
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null, null);
            }

            JSONObject responseObject = new JSONObject();
            responseObject.put("success", true);
            responseObject.put("message", "Successfully downloaded the file");
            handleConnectorResponse(messageContext, responseVariable, overwriteBody, responseObject, null, null);

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
