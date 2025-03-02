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

import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.connection.ConnectionConfiguration;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.core.util.ConnectorUtils;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.Error;

/**
 * AzureConfig class to handle the Azure DataLake connector configuration
 */
public class AzureConfig extends AbstractConnector implements ManagedLifecycle {

    @Override
    public void init(SynapseEnvironment synapseEnvironment) {
        // Do nothing on deployment - configs unknown by that time
    }

    @Override
    public void destroy() {

        ConnectionHandler.getConnectionHandler().
                shutdownConnections(AzureConstants.CONNECTOR_NAME);
    }

    @Override
    public void connect(MessageContext messageContext) {

        String connectorName = AzureConstants.CONNECTOR_NAME;
        String connectionName;
        try {
            ConnectionConfiguration configuration = getConnectionConfigFromContext(messageContext);
            connectionName = configuration.getConnectionName();
            ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
            if (handler.checkIfConnectionExists(connectorName, connectionName)) {
                AzureStorageConnectionHandler connectionHandler = (AzureStorageConnectionHandler) handler
                        .getConnection(connectorName, connectionName);
                if (!connectionHandler.getConnectionConfig().equals(configuration)) {
                    connectionHandler.setConnectionConfig(configuration);
                }
            } else {
                AzureStorageConnectionHandler azureStorageConnectionHandler =
                        new AzureStorageConnectionHandler(configuration);
                handler.createConnection(AzureConstants.CONNECTOR_NAME, connectionName, azureStorageConnectionHandler);
            }
        } catch (ConnectException e) {
            this.log.error(Error.CONNECTION_ERROR.getErrorMessage(), e);
            messageContext.setProperty("ERROR_CODE", Error.CONNECTION_ERROR.getErrorCode());
            messageContext.setProperty("ERROR_MESSAGE", Error.CONNECTION_ERROR.getErrorMessage());
            throw new SynapseException(Error.CONNECTION_ERROR.getErrorMessage(), e);
        } catch (Exception e) {
            this.log.error(Error.GENERAL_ERROR.getErrorMessage(), e);
            messageContext.setProperty("ERROR_CODE", Error.GENERAL_ERROR.getErrorCode());
            messageContext.setProperty("ERROR_MESSAGE", Error.GENERAL_ERROR.getErrorMessage());
            throw new SynapseException(Error.GENERAL_ERROR.getErrorMessage(), e);
        }
    }

    /**
     * Get connection configuration from the message context
     *
     * @param msgContext Message context
     * @return Connection configuration
     * @throws ConnectException If an error occurs while getting the connection configuration
     */
    private ConnectionConfiguration getConnectionConfigFromContext(MessageContext msgContext)
            throws ConnectException {

        String connectionName = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.CONNECTION_NAME);
        String endpointProtocol = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.PROTOCOL);
        String accountName = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.ACCOUNT_NAME);
        String accountKey = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.ACCOUNT_KEY);
        String clientId = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.CLIENT_ID);
        String clientSecret = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.CLIENT_SECRET);
        String tenantId = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.TENANT_ID);
        String sasToken = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.SAS_TOKEN);

        ConnectionConfiguration connectionConfig = new ConnectionConfiguration();
        connectionConfig.setConnectionName(connectionName);
        connectionConfig.setAccountKey(accountKey);
        connectionConfig.setAccountName(accountName);
        connectionConfig.setEndpointProtocol(endpointProtocol);
        connectionConfig.setClientID(clientId);
        connectionConfig.setClientSecret(clientSecret);
        connectionConfig.setTenantID(tenantId);
        connectionConfig.setSasToken(sasToken);
        return connectionConfig;
    }
}
