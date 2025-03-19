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
 * Enum representing error codes and messages for the Microsoft Azure storage connector.
 */
public enum Error {

    CONNECTION_ERROR("710001", "MS_AZURE_DATALAKE_GEN2:CONNECTION_ERROR"),
    INVALID_CONFIGURATION("710002", "MS_AZURE_DATALAKE_GEN2:INVALID_CONFIGURATION"),
    MISSING_PARAMETERS("710003", "MS_AZURE_DATALAKE_GEN2:MISSING_PARAMETERS"),
    AUTHENTICATION_ERROR("710004", "MS_AZURE_DATALAKE_GEN2:AUTHENTICATION_ERROR"),
    FILE_ALREADY_EXISTS_ERROR("710005", "MS_AZURE_DATALAKE_GEN2:FILE_ALREADY_EXISTS_ERROR"),
    FILE_IO_ERROR("710006", "MS_AZURE_DATALAKE_GEN2:FILE_IO_ERROR"),
    DATA_LAKE_STORAGE_GEN2_ERROR("710007", "MS_AZURE_DATALAKE_GEN2:DATA_LAKE_STORAGE_GEN2_ERROR"),
    FILE_PERMISSION_ERROR("710008", "MS_AZURE_DATALAKE_GEN2:FILE_PERMISSION_ERROR"),
    GENERAL_ERROR("710009", "MS_AZURE_DATALAKE_GEN2:GENERAL_ERROR"),
    FILE_SYSTEM_DOES_NOT_EXIST("710010", "MS_AZURE_DATALAKE_GEN2:FILE_SYSTEM_DOES_NOT_EXIST"),
    INVALID_JSON("710011", "MS_AZURE_DATALAKE_GEN2:INVALID_JSON"),
    TIMEOUT_ERROR("710012", "MS_AZURE_DATALAKE_GEN2:TIMEOUT_ERROR"),
    DIRECTORY_NOT_FOUND_ERROR("710013", "MS_AZURE_DATALAKE_GEN2:DIRECTORY_NOT_FOUND_ERROR"),
    FILE_DOES_NOT_EXIST("710014", "MS_AZURE_DATALAKE_GEN2:FILE_DOES_NOT_EXIST"),
    No_SUCH_ALGORITHM("710015", "MS_AZURE_DATALAKE_GEN2:No_SUCH_ALGORITHM"),
    IO_EXCEPTION("710016", "MS_AZURE_DATALAKE_GEN2:IO_EXCEPTION"),
    DIRECTORY_ALREADY_EXISTS_ERROR("700717", "MS_AZURE_DATALAKE_GEN2:DIRECTORY_ALREADY_EXISTS_ERROR");

    private final String code;
    private final String message;

    /**
     * Create an error code.
     *
     * @param code    error code represented by number
     * @param message error message
     */
    Error(String code, String message) {

        this.code = code;
        this.message = message;
    }

    public String getErrorCode() {

        return this.code;
    }

    public String getErrorMessage() {

        return this.message;
    }
}
