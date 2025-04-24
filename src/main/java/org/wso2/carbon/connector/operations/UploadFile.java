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
import com.azure.storage.common.ParallelTransferOptions;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.options.FileParallelUploadOptions;
import org.apache.synapse.MessageContext;
import org.apache.synapse.util.InlineExpressionUtil;
import org.json.JSONObject;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;
import org.wso2.carbon.connector.util.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashMap;

/**
 * Uploads a file to Azure Data Lake Storage.
 */
public class UploadFile extends AbstractAzureMediator {

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {

        String connectionName = getProperty(messageContext, AzureConstants.CONNECTION_NAME, String.class, false);
        String fileSystemName =
                getMediatorParameter(messageContext, AzureConstants.FILE_SYSTEM_NAME, String.class, false);
        String filePathToUpload =
                getMediatorParameter(messageContext, AzureConstants.FILE_PATH_TO_UPLOAD, String.class, false);
        String inputType = getMediatorParameter(messageContext, AzureConstants.INPUT_TYPE, String.class, false);
        String localFilePath =
                getMediatorParameter(messageContext, AzureConstants.LOCAL_FILE_PATH, String.class,
                        !((AzureConstants.L_LOCAL_FILE_PATH).equals(inputType)));
        String textContent =
                getMediatorParameter(messageContext, AzureConstants.TEXT_CONTENT, String.class,
                        true);
        String contentLanguage =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_LANGUAGE, String.class, true);
        String contentType = getMediatorParameter(messageContext, AzureConstants.CONTENT_TYPE, String.class, true);
        String contentEncoding =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_ENCODING, String.class, true);
        String contentDisposition =
                getMediatorParameter(messageContext, AzureConstants.CONTENT_DISPOSITION, String.class, true);
        String cacheControl = getMediatorParameter(messageContext, AzureConstants.CACHE_CONTROL, String.class, true);
        Integer blockSize = getMediatorParameter(messageContext, AzureConstants.BLOCK_SIZE, Integer.class, true);
        Integer maxSingleUploadSize =
                getMediatorParameter(messageContext, AzureConstants.MAX_SINGLE_UPLOAD_SIZE, Integer.class, true);
        Integer maxConcurrency =
                getMediatorParameter(messageContext, AzureConstants.MAX_CONCURRENCY, Integer.class, true);
        String metadata = getMediatorParameter(messageContext, AzureConstants.METADATA, String.class, true);
        Integer timeout = getMediatorParameter(messageContext, AzureConstants.TIMEOUT, Integer.class, true);

        Long maxSingleUploadSizeL =
                maxSingleUploadSize != null ? maxSingleUploadSize.longValue() * 1024L * 1024L : null;
        Long blockSizeL = blockSize != null ? blockSize.longValue() * 1024L * 1024L : null;

        try {

            DataLakeFileClient dataLakeFileClient =
                    getDataLakeFileClient(connectionName, fileSystemName, filePathToUpload);
            ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
                    .setBlockSizeLong(blockSizeL)
                    .setMaxConcurrency(maxConcurrency)
                    .setMaxSingleUploadSizeLong(maxSingleUploadSizeL);

            metadata = InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, metadata);

            HashMap<String, String> metadataMap = new HashMap<>();

            if (metadata != null && !metadata.isEmpty()) {
                Utils.addDataToMapFromArrayString(metadata, metadataMap);
            }

            PathHttpHeaders headers = new PathHttpHeaders()
                    .setCacheControl(cacheControl)
                    .setContentType(contentType)
                    .setContentDisposition(contentDisposition)
                    .setContentEncoding(contentEncoding)
                    .setContentLanguage(contentLanguage);

            Response<?> response = null;

            if (localFilePath != null && textContent == null) {
                byte[] fileContent = Files.readAllBytes(Paths.get(localFilePath));

                response = dataLakeFileClient.uploadFromFileWithResponse(
                        localFilePath,
                        parallelTransferOptions,
                        headers.setContentMd5(MessageDigest.getInstance("MD5").digest(fileContent)),
                        metadataMap,
                        null,
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null,
                        null);
            } else if (textContent != null && localFilePath == null) {
                response = dataLakeFileClient.uploadWithResponse(
                        new FileParallelUploadOptions(BinaryData.fromString(textContent))
                                .setHeaders(headers.setContentMd5(
                                        MessageDigest.getInstance("MD5").digest(textContent.getBytes())))
                                .setParallelTransferOptions(parallelTransferOptions)
                                .setMetadata(metadataMap),
                        timeout != null ? Duration.ofSeconds(timeout.longValue()) : null,
                        null);
            }

            if (response != null && response.getStatusCode() == 200) {
                JSONObject responseObject = new JSONObject();
                responseObject.put(AzureConstants.STATUS, true);
                responseObject.put(AzureConstants.MESSAGE, "Successfully uploaded the file");
                handleConnectorResponse(messageContext, responseVariable, overwriteBody, responseObject, null, null);
            }
            // No 'else' block is needed because if the upload file operation fails,
            // the SDK throws an exception. We only handle the success case explicitly
            // (status code 200) and let exceptions propagate for error handling.

        } catch (DataLakeStorageException e) {
            handleConnectorException(Error.DATA_LAKE_STORAGE_GEN2_ERROR, messageContext, e);
        } catch (ConnectException e) {
            handleConnectorException(Error.CONNECTION_ERROR, messageContext, e);
        } catch (NoSuchAlgorithmException e) {
            handleConnectorException(Error.No_SUCH_ALGORITHM, messageContext, e);
        } catch (IOException e) {
            handleConnectorException(Error.IO_EXCEPTION, messageContext, e);
        } catch (Exception e) {
            handleConnectorException(Error.GENERAL_ERROR, messageContext, e);
        }
    }
}
