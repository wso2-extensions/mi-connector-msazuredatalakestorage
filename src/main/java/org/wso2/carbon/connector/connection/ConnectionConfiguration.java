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

import org.apache.commons.lang3.StringUtils;
import org.wso2.carbon.connector.core.ConnectException;

/**
 * Connection Configuration
 */
public class ConnectionConfiguration {

    private String connectionName;
    private String accountName;
    private String accountKey;
    private String clientID;
    private String clientSecret;
    private String tenantID;
    private String endpointProtocol;
    private String sasToken;

    public String getConnectionName() {

        return connectionName;
    }

    public void setConnectionName(String connectionName) throws ConnectException {

        if (StringUtils.isNotBlank(connectionName)) {
            this.connectionName = connectionName;
        } else {
            throw new ConnectException("Mandatory parameter 'connectionName' is not set.");
        }
    }

    public String getAccountName() {

        return accountName;
    }

    public void setAccountName(String accountName) {

        this.accountName = accountName;
    }

    public String getAccountKey() {

        return accountKey;
    }

    public void setAccountKey(String accountKey) {

        this.accountKey = accountKey;
    }

    public String getClientID() {

        return clientID;
    }

    public void setClientID(String clientID) {

        this.clientID = clientID;
    }

    public String getClientSecret() {

        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {

        this.clientSecret = clientSecret;
    }

    public String getTenantID() {

        return tenantID;
    }

    public void setTenantID(String tenantID) {

        this.tenantID = tenantID;
    }

    public String getEndpointProtocol() {

        return endpointProtocol;
    }

    public void setEndpointProtocol(String endpointProtocol) {

        this.endpointProtocol = endpointProtocol;
    }

    public String getSasToken() {
        return sasToken;
    }

    public void setSasToken(String sasToken) {
        this.sasToken = sasToken;
    }

}
