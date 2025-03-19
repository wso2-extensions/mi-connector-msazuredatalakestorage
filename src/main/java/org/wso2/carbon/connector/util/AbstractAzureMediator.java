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
import org.apache.axis2.AxisFault;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.data.connector.ConnectorResponse;
import org.apache.synapse.data.connector.DefaultConnectorResponse;
import org.wso2.carbon.connector.core.AbstractConnector;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract class for handling common Azure operations.
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
        return getValue(parameter, parameterName, type, isOptional, messageContext, "Parameter");
    }

    protected <T> T getProperty(
            MessageContext messageContext, String propertyName, Class<T> type, boolean isOptional) {
        Object property = messageContext.getProperty(propertyName);
        return getValue(property, propertyName, type, isOptional, messageContext, "Property");
    }

    private <T> T getValue(Object value, String name, Class<T> type, boolean isOptional,
                           MessageContext messageContext, String valueType) {
        if (!isOptional && (value == null || value.toString().isEmpty())) {
            handleException(String.format("%s %s is not provided", valueType, name), messageContext);
        } else if (value == null || value.toString().isEmpty()) {
            return null;
        }

        try {
            return parse(Objects.requireNonNull(value).toString(), type);
        } catch (IllegalArgumentException e) {
            handleException(String.format("%s %s is not of type %s", valueType, name, type.getName()), messageContext);
        }

        return null;
    }


    /**
     * Creates and returns a {@link DataLakeRequestConditions} object with the specified conditions.
     *
     * @param leaseId the lease ID to be set in the request conditions (can be {@code null})
     * @param ifMatch an ETag value that must match for the request to succeed (can be {@code null})
     * @param ifNoneMatch an ETag value that must not match for the request to succeed (can be {@code null})
     * @param ifModifiedSince a timestamp in ISO-8601 format; the request succeeds only if the resource
     *                        has been modified since this time (can be {@code null})
     * @param ifUnmodifiedSince a timestamp in ISO-8601 format; the request succeeds only if the resource
     *                          has not been modified since this time (can be {@code null})
     * @return a {@link DataLakeRequestConditions} object with the provided conditions
     * @throws DateTimeParseException if {@code ifModifiedSince} or {@code ifUnmodifiedSince} are not in a valid ISO-8601 format
     */
    protected DataLakeRequestConditions getRequestConditions(String leaseId, String ifMatch, String ifNoneMatch,
                                                             String ifModifiedSince, String ifUnmodifiedSince) {

        try {
            return new DataLakeRequestConditions()
                    .setLeaseId(leaseId)
                    .setIfMatch(ifMatch)
                    .setIfModifiedSince(ifModifiedSince != null ? OffsetDateTime.parse(ifModifiedSince) : null)
                    .setIfNoneMatch(ifNoneMatch)
                    .setIfUnmodifiedSince(ifUnmodifiedSince != null ? OffsetDateTime.parse(ifUnmodifiedSince) : null);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid date-time format", e);
        }
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

        if (overwriteBody != null && overwriteBody) {

            org.apache.axis2.context.MessageContext axisMsgCtx =
                    ((Axis2MessageContext) messageContext).getAxis2MessageContext();
            try {
                JsonUtil.getNewJsonPayload(axisMsgCtx, payload.toString(), true, true);

            } catch (AxisFault e) {
                handleException("Error setting response payload", e, messageContext);
            }

            axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE,
                    AzureConstants.JSON_CONTENT_TYPE);
            axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.CONTENT_TYPE,
                    AzureConstants.JSON_CONTENT_TYPE);

        } else {
            response.setPayload(payload);
        }
        response.setHeaders(headers);
        response.setAttributes(attributes);
        messageContext.setVariable(responseVariable, response);
    }

    /**
     * Logs the given error code and message along with the provided exception,
     * sets the corresponding error properties in the {@link MessageContext},
     * and throws a {@link SynapseException} with the error message and cause.
     *
     * @param code the {@link Error} object containing the error code and message
     * @param mc the {@link MessageContext} in which the error properties should be set
     * @param e the {@link Throwable} exception that caused this error
     * @throws SynapseException always thrown with the error message and cause from the provided {@link Error} object
     */
    public void handleConnectorException(Error code, MessageContext mc, Throwable e) {

        this.log.error(code.getErrorMessage(), e);

        mc.setProperty(SynapseConstants.ERROR_CODE, code.getErrorCode());
        mc.setProperty(SynapseConstants.ERROR_MESSAGE, code.getErrorMessage());
        throw new SynapseException(code.getErrorMessage(), e);
    }

    /**
     * Logs the given error code and message, sets the corresponding error properties in the {@link MessageContext},
     * and throws a {@link SynapseException} with the error message.
     *
     * @param code the {@link Error} object containing the error code and message
     * @param mc the {@link MessageContext} in which the error properties should be set
     * @throws SynapseException always thrown with the error message from the provided {@link Error} object
     */
    public void handleConnectorException(Error code, MessageContext mc) {

        this.log.error(code.getErrorMessage());
        mc.setProperty(SynapseConstants.ERROR_CODE, code.getErrorCode());
        mc.setProperty(SynapseConstants.ERROR_MESSAGE, code.getErrorMessage());
        throw new SynapseException(code.getErrorMessage());
    }

    /**
     * Returns the corresponding {@link LeaseAction} constant for the given lease action string.
     *
     * @param leaseAction the lease action as a string (e.g., "Acquire", "Auto Renew", "Acquire Release", "Release")
     * @return the corresponding {@link LeaseAction} constant, or {@code null} if the input is {@code null}, empty,
     *         or does not match any known lease action.
     */
    public LeaseAction getLeaseAction(String leaseAction) {
        if (leaseAction == null || leaseAction.isEmpty()) {
            return null;
        }

        switch (leaseAction) {
            case "Acquire":
                return LeaseAction.ACQUIRE;
            case "Auto Renew":
                return LeaseAction.AUTO_RENEW;
            case "Acquire Release":
                return LeaseAction.ACQUIRE_RELEASE;
            case "Release":
                return LeaseAction.RELEASE;
            default:
                return null;
        }
    }


}
