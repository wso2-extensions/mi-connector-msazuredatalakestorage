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
    public static final String ACCOUNT_NAME_PARAM = "AccountName=";
    public static final String ACCOUNT_KEY_PARAM = "AccountKey=";
    public static final String PROTOCOL_KEY_PARAM = "DefaultEndpointsProtocol=";
    public static final String SEMICOLON = ";";
    public static final String LOCAL_FILE_PATH = "localFilePath";
    public static final String FILE_PATH_TO_UPLOAD = "filePathToUpload";
    public static final String TEXT_CONTENT = "textContent";
    public static final String FILE_PATH_TO_DELETE = "filePathToDelete";
    public static final String FILE_PATH_TO_RENAME = "filePathToRename";
    public static final String NEW_FILE_PATH = "newFilePath";
    public static final String FILE_PATH_TO_ADD_META_DATA = "filePathToAddMetaData";
    public static final String PATH = "path";
    public static final String METADATA = "metadata";
    public static final String DFS_ENDPOINT_SUFFIX = ".dfs.core.windows.net";
    public static final String HTTPS_PROTOCOL = "https://";
    public static final String RESPONSE_VARIABLE = "responseVariable";
    public static final String OVERWRITE_BODY = "overwriteBody";
    public static final String JSON_CONTENT_TYPE = "application/json";
    public static final String TIMEOUT = "timeout";
    public static final String PREFIX = "prefix";
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
    public static final String MAX_SINGLE_UPLOAD_SIZE = "maxSingleUploadSize";
    public static final String MAX_CONCURRENCY = "maxConcurrency";
    public static final String PERMISSIONS = "permissions";
    public static final String UMASK = "umask";
    public static final String OWNER = "owner";
    public static final String GROUP = "group";
    public static final String SOURCE_LEASE_ID = "sourceLeaseId";
    public static final String RANGE_GET_CONTENT_MD5 = "rangeGetContentMd5";
    public static final String MAX_RESULTS_PER_PAGE = "maxResultsPerPage";
    public static final String RETRIEVE_DELETED = "retrieveDeleted";
    public static final String RETRIEVE_METADATA = "retrieveMetadata";
    public static final String DESTINATION_LEASE_ID = "destinationLeaseId";
    public static final String SOURCE_IF_MATCH = "sourceIfMatch";
    public static final String SOURCE_IF_NONE_MATCH = "sourceIfNoneMatch";
    public static final String SOURCE_IF_MODIFIED_SINCE = "sourceIfModifiedSince";
    public static final String SOURCE_IF_UNMODIFIED_SINCE = "sourceIfUnmodifiedSince";
    public static final String DESTINATION_IF_MATCH = "destinationIfMatch";
    public static final String DESTINATION_IF_NONE_MATCH = "destinationIfNoneMatch";
    public static final String DESTINATION_IF_MODIFIED_SINCE = "destinationIfModifiedSince";
    public static final String DESTINATION_IF_UNMODIFIED_SINCE = "destinationIfUnmodifiedSince";
    public static final String FILE_PATH_TO_APPEND = "filePathToAppend";
    public static final String FLUSH = "flush";
    public static final String LEASE_ACTION = "leaseAction";
    public static final String LEASE_DURATION = "leaseDuration";
    public static final String INPUT_TYPE = "inputType";
    public static final String L_TEXT_CONTENT= "Text Content";
    public static final String L_LOCAL_FILE_PATH = "Local File";
    public static final String PROPOSED_LEASE_ID = "proposedLeaseId";
    public static final String FILE_PATH_TO_FLUSH = "filePathToFlush";
    public static final String FILE_LENGTH = "fileLength";
    public static final String UNCOMMITTED_DATA_RETAINED = "uncommittedDataRetained";
    public static final String SAS_TOKEN = "sasToken";
    private AzureConstants() {

    }
}
