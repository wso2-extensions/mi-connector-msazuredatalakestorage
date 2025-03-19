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

package org.wso2.carbon.connector.connection;

import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import org.apache.commons.lang.StringUtils;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.Connection;
import org.wso2.carbon.connector.core.connection.ConnectionConfig;
import org.wso2.carbon.connector.util.AbstractAzureMediator;
import org.wso2.carbon.connector.util.AzureConstants;

/**
 * Handles the connection to Azure Storage.
 */
public class AzureStorageConnectionHandler implements Connection {

    private ConnectionConfiguration connectionConfig;
    private DataLakeServiceClient dataLakeServiceClient;

    public AzureStorageConnectionHandler(ConnectionConfiguration fsConfig) {

        this.connectionConfig = fsConfig;
    }

    /**
     * Retrieves the DataLakeServiceClient instance. If the instance is not already created,
     * it initializes a new one using the provided connection configuration.
     *
     * @return an instance of DataLakeServiceClient.
     * @throws ConnectException if there is an issue creating the client instance.
     */
    public DataLakeServiceClient getDataLakeServiceClient() throws ConnectException {

        if (dataLakeServiceClient == null) {
            dataLakeServiceClient = createNewDataLakeServiceClientInstance(this.connectionConfig);
        }
        return dataLakeServiceClient;
    }

   /**
    * Retrieves the connection configuration.
    *
    * @return an instance of ConnectionConfiguration containing the connection settings.
    */
    public ConnectionConfiguration getConnectionConfig() {

        return connectionConfig;
    }

    /**
     * Set the connection configuration
     *
     * @param connectionConfig ConnectionConfiguration object
     */
    public void setConnectionConfig(ConnectionConfiguration connectionConfig)
            throws ConnectException {

        this.connectionConfig = connectionConfig;
        dataLakeServiceClient = createNewDataLakeServiceClientInstance(this.connectionConfig);
    }

   /**
    * Creates a new instance of `DataLakeServiceClient`.
    *
    * This method initializes a new `DataLakeServiceClient` instance using the provided
    * connection configuration. It supports different authentication methods including
    * OAuth2, Access Key, and Shared Access Signature (SAS) Token.
    *
    * @param config the `ConnectionConfiguration` object containing the connection settings.
    * @return a new instance of `DataLakeServiceClient`.
    * @throws ConnectException if there is an issue creating the client instance or if required
    *                          authentication parameters are missing.
    */
    private DataLakeServiceClient createNewDataLakeServiceClientInstance(ConnectionConfiguration config)
            throws ConnectException {

        String clientId = config.getClientID();
        String clientSecret = config.getClientSecret();
        String tenantId = config.getTenantID();
        String accountName = config.getAccountName();
        String accountKey = config.getAccountKey();
        String sasToken = config.getSasToken();

        if (StringUtils.isEmpty(accountName)) {
            throw new ConnectException("Missing account name. Please provide a valid account name to proceed.");
        }

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder()
                .httpClient(new NettyAsyncHttpClientBuilder().build());

        if (StringUtils.isNotEmpty(clientId) && StringUtils.isNotEmpty(clientSecret) &&
                StringUtils.isNotEmpty(tenantId)) {
            ClientSecretCredential credential = new ClientSecretCredentialBuilder()
                    .httpClient(new NettyAsyncHttpClientBuilder().build())
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .tenantId(tenantId)
                    .build();

            return builder.credential(credential)
                    .endpoint(AzureConstants.HTTPS_PROTOCOL + accountName + AzureConstants.DFS_ENDPOINT_SUFFIX)
                    .buildClient();
        }

        if (StringUtils.isNotEmpty(accountKey)) {
            return builder.connectionString(
                            AbstractAzureMediator.getStorageConnectionString(accountName, accountKey, AzureConstants.HTTPS))
                    .buildClient();
        }

        if (StringUtils.isNotEmpty(sasToken)) {
            return builder.endpoint(AzureConstants.HTTPS_PROTOCOL + accountName + AzureConstants.DFS_ENDPOINT_SUFFIX)
                    .sasToken(sasToken)
                    .buildClient();
        }

        throw new ConnectException("Missing authentication parameters. " +
                "If the access type is OAuth2, you must provide client ID, tenant ID, and client secret. " +
                "If the access type is Access Key, you must provide an account key. " +
                "If the access type is Shared Access Signature Token, you must provide a SAS token.");
    }

    @Override
    public void connect(ConnectionConfig connectionConfig) {

    }

    @Override
    public void close() {
        // Close the connection
    }

}
