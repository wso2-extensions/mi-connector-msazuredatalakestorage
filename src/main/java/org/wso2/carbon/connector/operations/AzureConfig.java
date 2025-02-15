package org.wso2.carbon.connector.operations;

import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.connection.AzureStorageConnectionHandler;
import org.wso2.carbon.connector.connection.ConnectionConfiguration;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.core.util.ConnectorUtils;
import org.wso2.carbon.connector.exceptions.InvalidConfigurationException;
import org.wso2.carbon.connector.util.AzureConstants;
import org.wso2.carbon.connector.util.AzureUtil;
import org.wso2.carbon.connector.util.Error;


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
    public void connect(MessageContext messageContext) throws ConnectException{
        String connectorName = AzureConstants.CONNECTOR_NAME;
        String connectionName = "";
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
                AzureStorageConnectionHandler azureStorageConnectionHandler = new AzureStorageConnectionHandler(configuration);
                handler.createConnection(AzureConstants.CONNECTOR_NAME, connectionName, azureStorageConnectionHandler);
            }
        } catch (InvalidConfigurationException e) {
            AzureUtil.setErrorPropertiesToMessage(messageContext, Error.INVALID_CONFIGURATION, e.getMessage());
            handleException("[" + connectionName + "] Failed to initiate Azure DataLake connector configuration.", e,
                    messageContext);
        }
    }

    private ConnectionConfiguration getConnectionConfigFromContext(MessageContext msgContext) throws InvalidConfigurationException {

        String connectionName = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.CONNECTION_NAME);
        String endpointProtocol = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.PROTOCOL);
        String accountName = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.ACCOUNT_NAME);
        String accountKey = (String) ConnectorUtils.
                lookupTemplateParamater(msgContext, AzureConstants.ACCOUNT_KEY);
        String clientId = (String) ConnectorUtils.lookupTemplateParamater(msgContext, AzureConstants.CLIENT_ID);
        String clientSecret = (String) ConnectorUtils.lookupTemplateParamater(msgContext, AzureConstants.CLIENT_SECRET);
        String tenantId = (String) ConnectorUtils.lookupTemplateParamater(msgContext, AzureConstants.TENANT_ID);

        ConnectionConfiguration connectionConfig = new ConnectionConfiguration();
        connectionConfig.setConnectionName(connectionName);
        connectionConfig.setAccountKey(accountKey);
        connectionConfig.setAccountName(accountName);
        connectionConfig.setEndpointProtocol(endpointProtocol);
        connectionConfig.setClientID(clientId);
        connectionConfig.setClientSecret(clientSecret);
        connectionConfig.setTenantID(tenantId);
        return connectionConfig;
    }

}
