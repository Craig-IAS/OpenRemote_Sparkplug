package org.openremote.manager.mqtt;


import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.eclipse.tahu.exception.TahuException;
import org.eclipse.tahu.host.CommandPublisher;
import org.eclipse.tahu.host.TahuPayloadHandler;
import org.eclipse.tahu.message.PayloadDecoder;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.mqtt.MqttClientId;
import org.eclipse.tahu.mqtt.MqttServerName;
import org.keycloak.KeycloakSecurityContext;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.openremote.container.message.MessageBrokerService;
import org.openremote.manager.security.ManagerIdentityService;
import org.openremote.manager.security.ManagerKeycloakIdentityProvider;
import org.openremote.model.Container;
import org.openremote.model.syslog.SyslogCategory;;
import java.util.Set;
import java.util.logging.Logger;
import static org.openremote.model.syslog.SyslogCategory.API;
import org.openremote.manager.asset.AssetStorageService;
import org.openremote.container.timer.TimerService;
public class SparkplugMQTTHandler extends MQTTHandler {

    private static final Logger LOG = SyslogCategory.getLogger(API, SparkplugMQTTHandler.class);
    private static final String SPARKPLUG_NAMESPACE = "spBv10";

    protected AssetStorageService assetService;
    protected TimerService timerService;

    protected MqttEventHandler mqttEventHandler;
    protected MqttCommandPublisher commandPublisher;

    protected SparkplugBPayloadDecoder decoder;
    protected TahuPayloadHandler payloadHandler;

    protected MqttServerName mqttServerName;
    protected MqttClientId mqttClientId;

    public SparkplugMQTTHandler() throws TahuException {

    }

    @Override
    protected boolean topicMatches(Topic topic) {
        String topicString = topicTokenIndexToString(topic, 0) + topicTokenIndexToString(topic, 1);
        if (SPARKPLUG_NAMESPACE.equalsIgnoreCase(topicString)){
            getLogger().info("Matches Sparkplug Handler");
            return true;
        }
        return false;
    }

    public void start(Container container) throws Exception {
        super.start(container);
        getLogger().info("Starting Sparkplug Handler");
        ManagerIdentityService identityService = container.getService(ManagerIdentityService.class);
        assetService = container.getService(AssetStorageService.class);
        timerService = container.getService(TimerService.class);
        messageBrokerService = container.getService(MessageBrokerService.class);
        mqttEventHandler = new MqttEventHandler(assetService,messageBrokerService);
        commandPublisher = new MqttCommandPublisher();
        mqttServerName = new MqttServerName("openremote");
        mqttClientId = new MqttClientId("openremote",true);
        decoder = new SparkplugBPayloadDecoder();
        payloadHandler = new TahuPayloadHandler(mqttEventHandler, commandPublisher, decoder);

        //TODO: publish mqtt birth message as per sparkplug spec
        ///TODO: sub to own topic as per sparkplug spec

        if (!identityService.isKeycloakEnabled()) {
            LOG.warning("MQTT connections are not supported when not using Keycloak identity provider");
            isKeycloak = false;
        } else {
            isKeycloak = true;
            identityProvider = (ManagerKeycloakIdentityProvider) identityService.getIdentityProvider();
        }


    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    public boolean canSubscribe(RemotingConnection connection,
            KeycloakSecurityContext securityContext, Topic topic) {
        getLogger().fine("canSubscribe");
        return true;
    }

    @Override
    public boolean canPublish(RemotingConnection connection,
            KeycloakSecurityContext securityContext, Topic topic) {
        getLogger().fine("canPublish");
        return true;
    }

    @Override
    public void onSubscribe(RemotingConnection connection, Topic topic) {

    }

    @Override
    public void onUnsubscribe(RemotingConnection connection, Topic topic) {

    }

    @Override
    public Set<String> getPublishListenerTopics() {
        getLogger().fine("getPublishListenerTopics");
        // topics are split by periods and "/" so we need to work around this for now
        return Set.of(
                "spBv1"+"/0/" + TOKEN_MULTI_LEVEL_WILDCARD
        );
    }

    @Override
    public void onPublish(RemotingConnection connection, Topic topic, ByteBuf body) {
        //log the topic and body
        getLogger().info("onPublish: " + topic + " " + body.toString());
        //The topic is being split by a period so for now we rebuild the topic
        //TODO: fix this somewhere else
        String topicString = topic.getString();
        topicString = topicString.replaceFirst("/", ".");
        String[] topicArray = topicString.split("/");

        MqttMessage message = new MqttMessage(extractReadableBytes(body));




        payloadHandler.handlePayload(topicString,topicArray,message,mqttServerName,mqttClientId);





    }
    private byte[] extractReadableBytes(ByteBuf buffer) {
        if (buffer.hasArray()) {
            int start = buffer.arrayOffset() + buffer.readerIndex();
            int length = buffer.readableBytes();

            byte[] array = new byte[length];
            System.arraycopy(buffer.array(), start, array, 0, length);
            return array;
        } else {
            byte[] array = new byte[buffer.readableBytes()];
            buffer.getBytes(buffer.readerIndex(), array);
            return array;
        }
    }

    @Override
    public boolean checkCanPublish(RemotingConnection connection, KeycloakSecurityContext securityContext, Topic topic) {
        // Skip standard checks
        return canPublish(connection, securityContext, topic);

    }

}
