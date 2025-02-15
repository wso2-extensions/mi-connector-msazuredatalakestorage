/*
 *  Copyright (c) 2023, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.connector.connection;

import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import org.apache.commons.lang.StringUtils;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.Connection;
import org.wso2.carbon.connector.exceptions.InvalidConfigurationException;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.AzureUtil;

public class AzureStorageConnectionHandler implements Connection {

    private ConnectionConfiguration connectionConfig;
    private DataLakeServiceClient dataLakeServiceClient;

    public AzureStorageConnectionHandler(ConnectionConfiguration fsConfig) {
        this.connectionConfig = fsConfig;
    }

    /**
     * @return an instance of BlobServiceClient
     */
    public DataLakeServiceClient getDataLakeServiceClient() throws ConnectException {

        if (dataLakeServiceClient == null) {
            dataLakeServiceClient = createNewDataLakeServiceClientInstance(this.connectionConfig);
        }
        return dataLakeServiceClient;
    }

    public ConnectionConfiguration getConnectionConfig() {

        return connectionConfig;
    }

    public void setConnectionConfig(ConnectionConfiguration connectionConfig) throws InvalidConfigurationException {

        this.connectionConfig = connectionConfig;
        dataLakeServiceClient = createNewDataLakeServiceClientInstance(this.connectionConfig);
    }

    private DataLakeServiceClient createNewDataLakeServiceClientInstance(ConnectionConfiguration config) throws InvalidConfigurationException {

        String clientId = config.getClientID();
        String clientSecret = config.getClientSecret();
        String tenantId = config.getTenantID();
        String accountName = config.getAccountName();
        String accountKey = config.getAccountKey();
        String endpointProtocol = config.getEndpointProtocol();

        if (StringUtils.isNotEmpty(clientId) && StringUtils.isNotEmpty(clientSecret)
                && StringUtils.isNotEmpty(tenantId) && StringUtils.isNotEmpty(accountName)) {
            ClientSecretCredential credential = new ClientSecretCredentialBuilder()
                    .httpClient(new NettyAsyncHttpClientBuilder().build())
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .tenantId(tenantId)
                    .build();
            return new DataLakeServiceClientBuilder()
                    .httpClient(new NettyAsyncHttpClientBuilder().build())
                    .credential(credential)
                    .endpoint(AzureConstants.HTTPS_PROTOCOL + accountName + AzureConstants.DFS_ENDPOINT_SUFFIX)
                    .buildClient();
        } else if (StringUtils.isNotEmpty(accountName) && StringUtils.isNotEmpty(accountKey)) {
            return new DataLakeServiceClientBuilder()
                    .httpClient(new NettyAsyncHttpClientBuilder().build())
                    .connectionString(AzureUtil.getStorageConnectionString(accountName, accountKey, endpointProtocol))
                    .buildClient();
        } else {
            throw new InvalidConfigurationException("Missing authentication parameters.");
        }
    }
}
