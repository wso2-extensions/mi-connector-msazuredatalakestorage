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

package org.wso2.carbon.connector.util;

import ca.uhn.hl7v2.model.v21.datatype.ST;
import org.bouncycastle.pqc.crypto.util.PQCOtherInfoGenerator;
import org.jruby.compiler.DAGBuilder;

/**
 * This class contains the azure connector specific constants.
 */
public class AzureConstants {
    public static final String CONNECTOR_NAME = "azureDataLake";
    public static final String CONNECTION_NAME = "name";
    public static final String ACCOUNT_NAME = "accountName";
    public static final String CLIENT_ID = "clientId";
    public static final String CLIENT_SECRET = "clientSecret";
    public static final String TENANT_ID = "tenantId";
    public static final String ACCOUNT_KEY = "accountKey";
    public static final String PROTOCOL = "defaultEndpointsProtocol";
    public static final String FILE_SYSTEM_NAME = "fileSystemName";
    public static final String DIRECTORY_NAME = "directoryName";
    public static final String NEW_DIRECTORY_NAME = "newDirectoryName";
    public static final String FILE_PATH = "filePath";
    public static final String FILE_PATH_TO_DOWNLOAD = "filePathToDownload";
    public static final String DOWNLOAD_LOCATION = "downloadLocation";
    public static final String CONTENT = "content";
    public static final String ACCOUNT_NAME_PARAM = "AccountName=";
    public static final String ACCOUNT_KEY_PARAM = "AccountKey=";
    public static final String PROTOCOL_KEY_PARAM = "DefaultEndpointsProtocol=";
    public static final String SEMICOLON = ";";
    public static final String NAMESPACE = "ns";
    public static final String AZURE_NAMESPACE = "http://org.wso2.esbconnectors.azuredatalakeconnector";
    public static final String FILE_SYSTEM = "fileSystem";
    public static final String LOCAL_FILE_PATH = "localFilePath";
    public static final String FILE_PATH_TO_UPLOAD = "filePathToUpload";
    public static final String FINAL_CONTENT_TYPE = "finalContentType";
    public static final String TEXT_CONTENT = "textContent";
    public static final String FILE_PATH_TO_DELETE = "filePathToDelete";
    public static final String FILE_PATH_TO_RENAME = "filePathToRename";
    public static final String NEW_FILE_PATH = "newFilePath";

    public static final String FILE_PATH_TO_ADD_META_DATA = "filePathToAddMetaData";

    public static final String PATH = "path";

    public static final String START_TAG = "<jsonObject><result><success>";
    public static final String END_TAG = "</success></result></jsonObject>";
    public static final String EMPTY_RESULT_TAG = "<result></></result>";
    public static final String METADATA = "metadata";
    public static final String START_TAG_ERROR = "<jsonObject><result><success>false</success><description>";
    public static final String END_TAG_ERROR = "</description></result></jsonObject>";
    public static final String DFS_ENDPOINT_SUFFIX = ".dfs.core.windows.net";
    public static final String HTTPS_PROTOCOL = "https://";
    public static final String PROPERTY_ERROR_CODE = "ERROR_CODE";
    public static final String PROPERTY_ERROR_MESSAGE = "ERROR_MESSAGE";
    public static final String PROPERTY_ERROR_DETAIL = "ERROR_DETAIL";
    public static final String RESPONSE_VARIABLE = "responseVariable";
    public static final String OVERWRITE_BODY = "overwriteBody";
    public static final String JSON_CONTENT_TYPE = "application/json";
    public static final String TIMEOUT = "timeout";
    public static final String PREFIX = "prefix";
    public static final String OVERWRITE = "overwrite";
    public static final String DELETE_CONTENTS = "deleteContents";
    public static final String RECURSIVE = "recursive";
    public static final String MAX_RESULTS = "maxResults";
    public static final String COUNT = "count";
    public static final String OFFSET = "offset";
    public static final String MAX_RETRY_REQUESTS = "maxRetryRequests";
    public static final String NEW_FILE_SYSTEM_NAME = "newFileSystemName";
    public static final String LEASE_ID = "leaseId";
    public static final String IF_UNMODIFIED_SINCE = "ifUnmodifiedSince";
    public static final String IF_MATCH = "ifMatch";
    public static final String IF_MODIFIED_SINCE = "ifModifiedSince";
    public static final String IF_NONE_MATCH = "ifNoneMatch";
    public static final String CONTENT_LANGUAGE = "contentLanguage";
    public static final String CONTENT_TYPE = "contentType";
    public static final String CONTENT_ENCODING = "contentEncoding";
    public static final String CONTENT_DISPOSITION = "contentDisposition";
    public static final String CACHE_CONTROL = "cacheControl";
    public static final String BLOCK_SIZE = "blockSize";
    public static final String MAX_SINGLE_UPLAOD_SIZE = "maxSingleUploadSize";
    public static final String MAX_CONCURRENCY = "maxConcurrency";

    private AzureConstants() {
    }
}
