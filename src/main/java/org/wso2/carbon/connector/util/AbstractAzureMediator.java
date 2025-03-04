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

import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.LeaseAction;
import com.google.gson.JsonParser;
import org.apache.axis2.AxisFault;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.data.connector.ConnectorResponse;
import org.apache.synapse.data.connector.DefaultConnectorResponse;
import org.wso2.carbon.connector.core.AbstractConnector;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract Azure Mediator class to handle common operations.
 */
public abstract class AbstractAzureMediator extends AbstractConnector {

    @SuppressWarnings("unchecked")
    public static <T> T parse(String value, Class<T> type) throws IllegalArgumentException {

        if (type == Integer.class) {
            return (T) Integer.valueOf(value);
        } else if (type == Double.class) {
            return (T) Double.valueOf(value);
        } else if (type == Boolean.class) {
            return (T) Boolean.valueOf(value);
        } else if (type == String.class) {
            return (T) value;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    /**
     * Get the connection string for the storage account.
     *
     * @param accountName Storage account name.
     * @param accountKey  Storage account key.
     * @param protocol    Protocol.
     * @return Connection string.
     */
    public static String getStorageConnectionString(String accountName, String accountKey, String protocol) {

        return AzureConstants.PROTOCOL_KEY_PARAM + protocol + AzureConstants.SEMICOLON +
                AzureConstants.ACCOUNT_NAME_PARAM + accountName + AzureConstants.SEMICOLON
                + AzureConstants.ACCOUNT_KEY_PARAM + accountKey;
    }

    abstract public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody);

    @Override
    public void connect(MessageContext messageContext) {

        String responseVariable = getMediatorParameter(
                messageContext, AzureConstants.RESPONSE_VARIABLE, String.class, false
                                                      );
        Boolean overwriteBody = getMediatorParameter(
                messageContext, AzureConstants.OVERWRITE_BODY, Boolean.class, false);
        execute(messageContext, responseVariable, overwriteBody);
    }

    protected <T> T getMediatorParameter(
            MessageContext messageContext, String parameterName, Class<T> type, boolean isOptional) {

        Object parameter = getParameter(messageContext, parameterName);
        if (!isOptional && (parameter == null || parameter.toString().isEmpty())) {
            handleException(String.format("Parameter %s is not provided", parameterName), messageContext);
        } else if (parameter == null || parameter.toString().isEmpty()) {
            return null;
        }

        try {
            return parse(Objects.requireNonNull(parameter).toString(), type);
        } catch (IllegalArgumentException e) {
            this.log.info(parameter + "mmmm" + parameter.getClass());
            handleException(String.format(
                    "Parameter %s is not of type %s", parameterName, type.getName()
                                         ), messageContext);
        }

        return null;
    }

    protected <T> T getProperty(
            MessageContext messageContext, String propertyName, Class<T> type, boolean isOptional) {

        Object property = messageContext.getProperty(propertyName);
        if (!isOptional && (property == null || property.toString().isEmpty())) {
            handleException(String.format("Property %s is not set", propertyName), messageContext);
        } else if (property == null || property.toString().isEmpty()) {
            return null;
        }

        try {
            return parse(Objects.requireNonNull(property).toString(), type);
        } catch (IllegalArgumentException e) {
            handleException(String.format(
                    "Property %s is not of type %s", propertyName, type.getName()
                                         ), messageContext);
        }

        return null;
    }

    /**
     * Get DataLakeRequestConditions object.
     *
     * @param leaseId           Lease ID.
     * @param ifMatch           If-Match.
     * @param ifNoneMatch       If-None-Match.
     * @param ifModifiedSince   If-Modified-Since.
     * @param ifUnmodifiedSince If-Unmodified-Since.
     * @return DataLakeRequestConditions object.
     */
    protected DataLakeRequestConditions getRequestConditions(String leaseId, String ifMatch, String ifNoneMatch,
                                                             String ifModifiedSince, String ifUnmodifiedSince) {

        return new DataLakeRequestConditions()
                .setLeaseId(leaseId)
                .setIfMatch(ifMatch)
                .setIfModifiedSince(ifModifiedSince != null ? OffsetDateTime.parse(ifModifiedSince) : null)
                .setIfNoneMatch(ifNoneMatch)
                .setIfUnmodifiedSince(ifUnmodifiedSince != null ? OffsetDateTime.parse(ifUnmodifiedSince) : null);
    }

    protected void handleConnectorResponse(MessageContext messageContext, String responseVariable,
                                           Boolean overwriteBody, Object payload,
                                           Map<String, Object> headers, Map<String, Object> attributes) {

        ConnectorResponse response = new DefaultConnectorResponse();
        if (payload == null) {
            // Empty json object
            payload = Map.of();
        }
        if (headers == null) {
            headers = Map.of();
        }
        if (attributes == null) {
            attributes = Map.of();
        }

        Object output;
        String jsonString = Utils.toJson(payload);
        if (payload instanceof List) {
            output = JsonParser.parseString(jsonString).getAsJsonArray();
        } else if (payload instanceof String || payload instanceof Boolean ||
                payload instanceof Long || payload instanceof Double) {
            output = payload;
        } else {
            // Convert Java object to JSON string
            output = JsonParser.parseString(jsonString).getAsJsonObject();
        }

        if (overwriteBody != null && overwriteBody) {
            org.apache.axis2.context.MessageContext axisMsgCtx =
                    ((Axis2MessageContext) messageContext).getAxis2MessageContext();
            try {
                JsonUtil.getNewJsonPayload(axisMsgCtx, jsonString, true, true);
            } catch (AxisFault e) {
                handleException("Error setting response payload", e, messageContext);
            }

            axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE,
                    AzureConstants.JSON_CONTENT_TYPE);
            axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.CONTENT_TYPE,
                    AzureConstants.JSON_CONTENT_TYPE);
        } else {
            response.setPayload(output);
        }
        response.setHeaders(headers);
        response.setAttributes(attributes);
        messageContext.setVariable(responseVariable, response);
    }

    public void handleConnectorException(Error code, MessageContext mc, Throwable e) {

        this.log.error(code.getErrorMessage(), e);

        mc.setProperty("ERROR_CODE", code.getErrorCode());
        mc.setProperty("ERROR_MESSAGE", code.getErrorMessage());
        throw new SynapseException(code.getErrorMessage(), e);
    }

    public void handleConnectorException(Error code, MessageContext mc) {

        this.log.error(code.getErrorMessage());

        mc.setProperty("ERROR_CODE", code.getErrorCode());
        mc.setProperty("ERROR_MESSAGE", code.getErrorMessage());
        throw new SynapseException(code.getErrorMessage());
    }

    public LeaseAction getLeaseAction(String leaseAction) {

        LeaseAction leaseActionConstant = null;

        switch (leaseAction) {
            case "Acquire":
                leaseActionConstant = LeaseAction.ACQUIRE;
                break;
            case "Auto Renew":
                leaseActionConstant = LeaseAction.AUTO_RENEW;
                break;
            case "Acquire Release":
                leaseActionConstant = LeaseAction.ACQUIRE_RELEASE;
                break;
            case "Release":
                leaseActionConstant = LeaseAction.RELEASE;
                break;
        }

        return leaseActionConstant;
    }

}
