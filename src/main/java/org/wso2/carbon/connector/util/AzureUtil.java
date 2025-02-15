/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.connector.util;

import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.exceptions.InvalidConfigurationException;

/**
 * This class contain required util methods for to Azure Storage connector.
 */
public class AzureUtil {
    public static String getStorageConnectionString(String accountName, String accountKey, String protocol) {
        return AzureConstants.PROTOCOL_KEY_PARAM + protocol + AzureConstants.SEMICOLON +
               AzureConstants.ACCOUNT_NAME_PARAM + accountName + AzureConstants.SEMICOLON
               + AzureConstants.ACCOUNT_KEY_PARAM + accountKey;
    }

    /**
     * Retrieves connection name from message context if configured as configKey attribute
     * or from the template parameter
     *
     * @param messageContext Message Context from which the parameters should be extracted from
     * @return connection name
     */
    public static String getConnectionName(MessageContext messageContext) throws InvalidConfigurationException {

        String connectionName = (String) messageContext.getProperty(AzureConstants.CONNECTION_NAME);
        if (connectionName == null) {
            throw new InvalidConfigurationException("Mandatory parameter 'connectionName' is not set.");
        }
        return connectionName;
    }

    public static String generateResultPayload(boolean status, String description) {
        if (status) {
            return AzureConstants.START_TAG + true + AzureConstants.END_TAG;
        }
        return AzureConstants.START_TAG_ERROR + description + AzureConstants.END_TAG_ERROR;
    }

    /**
     * Sets the error code and error detail in message
     *
     * @param messageContext Message Context
     * @param error          Error to be set
     */
    public static void setErrorPropertiesToMessage(MessageContext messageContext, Error error, String errorDetails) {

        messageContext.setProperty(AzureConstants.PROPERTY_ERROR_CODE, error.getErrorCode());
        messageContext.setProperty(AzureConstants.PROPERTY_ERROR_MESSAGE, error.getErrorMessage());
        messageContext.setProperty(AzureConstants.PROPERTY_ERROR_DETAIL, errorDetails);
    }

    /**
     * Get the error message
     *
     * @param statusCode The status code of the response
     * @return The error message
     */
    public static String getErrorMessage(String operation, int statusCode) {

        return operation + ": Received Status Code: " + statusCode + " from Azure DataLake Storage.";
    }
}
