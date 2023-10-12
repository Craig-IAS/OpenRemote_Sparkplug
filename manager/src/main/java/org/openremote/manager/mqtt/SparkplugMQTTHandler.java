package org.openremote.manager.mqtt;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.apache.camel.builder.RouteBuilder;
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
import org.openremote.manager.event.ClientEventService;
import org.openremote.manager.mqtt.DefaultMQTTHandler.SubscriberInfo;
import org.openremote.manager.security.ManagerIdentityService;
import org.openremote.manager.security.ManagerKeycloakIdentityProvider;
import org.openremote.model.Container;
import org.openremote.model.asset.Asset;
import org.openremote.model.asset.AssetEvent;
import org.openremote.model.asset.AssetFilter;
import org.openremote.model.attribute.AttributeEvent;
import org.openremote.model.event.TriggeredEventSubscription;
import org.openremote.model.event.shared.EventSubscription;
import org.openremote.model.event.shared.SharedEvent;
import org.openremote.model.syslog.SyslogCategory;;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.apache.camel.support.builder.PredicateBuilder.and;
import static org.openremote.manager.event.ClientEventService.CLIENT_INBOUND_QUEUE;
import static org.openremote.manager.event.ClientEventService.CLIENT_OUTBOUND_QUEUE;
import static org.openremote.manager.event.ClientEventService.HEADER_CONNECTION_TYPE;
import static org.openremote.manager.event.ClientEventService.HEADER_CONNECTION_TYPE_MQTT;
import static org.openremote.manager.mqtt.MQTTBrokerService.getConnectionIDString;
import static org.openremote.model.Constants.REALM_PARAM_NAME;
import static org.openremote.model.Constants.SESSION_CLOSE;
import static org.openremote.model.Constants.SESSION_KEY;
import static org.openremote.model.Constants.SESSION_OPEN;
import static org.openremote.model.Constants.SESSION_TERMINATOR;
import static org.openremote.model.attribute.AttributeEvent.HEADER_SOURCE;
import static org.openremote.model.attribute.AttributeEvent.Source.CLIENT;
import static org.openremote.model.syslog.SyslogCategory.API;
import org.openremote.manager.asset.AssetStorageService;
import org.openremote.container.timer.TimerService;
public class SparkplugMQTTHandler extends MQTTHandler {

    public static class SubscriberInfo {
        protected Map<String, Consumer<SharedEvent>> topicSubscriptionMap;

        public SubscriberInfo(String topic, Consumer<SharedEvent> subscriptionConsumer) {
            this.topicSubscriptionMap = new HashMap<>();
            this.topicSubscriptionMap.put(topic, subscriptionConsumer);
        }

        protected void add(String topic, Consumer<SharedEvent> subscriptionConsumer) {
            topicSubscriptionMap.put(topic, subscriptionConsumer);
        }

        protected int remove(String topic) {
            topicSubscriptionMap.remove(topic);
            return topicSubscriptionMap.size();
        }
    }

    protected final ConcurrentMap<String, SubscriberInfo>
            connectionSubscriberInfoMap = new ConcurrentHashMap<>();

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
    protected final Cache<String, ConcurrentHashSet<String>> authorizationCache = CacheBuilder.newBuilder()
            .maximumSize(100000)
            .expireAfterWrite(300000, TimeUnit.MILLISECONDS)
            .build();

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
        commandPublisher = new MqttCommandPublisher(assetService, timerService, mqttBrokerService);
        mqttServerName = new MqttServerName("openremote");
        mqttClientId = new MqttClientId("openremote",true);
        decoder = new SparkplugBPayloadDecoder();
        payloadHandler = new TahuPayloadHandler(mqttEventHandler, commandPublisher, decoder);

        if (!identityService.isKeycloakEnabled()) {
            LOG.warning("MQTT connections are not supported when not using Keycloak identity provider");
            isKeycloak = false;
        } else {
            isKeycloak = true;
            identityProvider = (ManagerKeycloakIdentityProvider) identityService.getIdentityProvider();
        }


    }

    @Override
    public void init(Container container) throws Exception {
        super.init(container);
        messageBrokerService.getContext().addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {

                // Route messages destined for MQTT clients
                from(CLIENT_OUTBOUND_QUEUE)
                        .routeId("ClientOutbound-SparkplugMQTTHandler")
                        .filter(and(
                                header(HEADER_CONNECTION_TYPE).isEqualTo(HEADER_CONNECTION_TYPE_MQTT),
                                body().isInstanceOf(TriggeredEventSubscription.class)
                        ))
                        .process(exchange -> {
                            // Get the subscriber consumer
                            String connectionID = exchange.getIn().getHeader(SESSION_KEY, String.class);
                            SubscriberInfo subscriberInfo = connectionSubscriberInfoMap.get(connectionID);
                            if (subscriberInfo != null) {
                                TriggeredEventSubscription<?> triggeredEventSubscription = exchange.getIn().getBody(TriggeredEventSubscription.class);
                                String topic = triggeredEventSubscription.getSubscriptionId();
                                // Should only be a single event in here
                                SharedEvent event = triggeredEventSubscription.getEvents().get(0);
                                Consumer<SharedEvent> eventConsumer = subscriberInfo.topicSubscriptionMap.get(topic);
                                if (eventConsumer != null) {
                                    eventConsumer.accept(event);
                                }
                            }
                        });
            }
        });
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
    @Override
    public void onConnect(RemotingConnection connection) {
        super.onConnect(connection);
        Map<String, Object> headers = prepareHeaders(null, connection);
        headers.put(SESSION_OPEN, true);

        // Put a close connection runnable into the headers for the client event service
        Runnable closeRunnable = () -> {
            if (mqttBrokerService != null) {
                LOG.fine("Calling client session closed force disconnect runnable: " + mqttBrokerService.connectionToString(connection));
                mqttBrokerService.doForceDisconnect(connection);
            }
        };
        headers.put(SESSION_TERMINATOR, closeRunnable);
        messageBrokerService.getFluentProducerTemplate()
                .withHeaders(headers)
                .to(CLIENT_INBOUND_QUEUE)
                .asyncSend();
    }

    @Override
    public void onDisconnect(RemotingConnection connection) {
        super.onDisconnect(connection);

        Map<String, Object> headers = prepareHeaders(null, connection);
        headers.put(SESSION_CLOSE, true);
        messageBrokerService.getFluentProducerTemplate()
                .withHeaders(headers)
                .to(CLIENT_INBOUND_QUEUE)
                .asyncSend();
        connectionSubscriberInfoMap.remove(getConnectionIDString(connection));
        authorizationCache.invalidate(getConnectionIDString(connection));
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

        //log the topic and body
        getLogger().info("onSubscribe: " + topic );
        //The topic is being split by a period so for now we rebuild the topic
        //TODO: fix this somewhere else
        String topicString = topic.getString().replaceFirst("/", ".");
        String[] topicArray = topicString.split("/");

        AssetFilter filter = buildAssetFilter(topicArray);

        Consumer<SharedEvent> eventConsumer = getSubscriptionEventConsumer(connection, topicArray);
        EventSubscription subscription = new EventSubscription(
                AttributeEvent.class,
                filter,
                topicString
        );

        Map<String, Object> headers = prepareHeaders(topicArray[1], connection);
        messageBrokerService.getFluentProducerTemplate()
                .withHeaders(headers)
                .withBody(subscription)
                .to(CLIENT_INBOUND_QUEUE)
                .asyncSend();
        synchronized (connectionSubscriberInfoMap) {
            connectionSubscriberInfoMap.compute(getConnectionIDString(connection), (connectionID, subscriberInfo) -> {
                if (subscriberInfo == null) {
                    return new SubscriberInfo(topicString, eventConsumer);
                } else {
                    subscriberInfo.add(topicString, eventConsumer);
                    return subscriberInfo;
                }
            });
        }




    }

    private Consumer<SharedEvent> getSubscriptionEventConsumer(RemotingConnection connection, String[] topicArray) {
        // Always publish asset/attribute messages with QoS 0
        //if topic length equals 4 then its a sparkplug node if it is 5 then its a device


        return ev -> {
            if (ev instanceof AttributeEvent attributeEvent) {
                try {
                    commandPublisher.publishCommandFromEvent(topicArray, attributeEvent);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        };
    }



    private AssetFilter buildAssetFilter(String[] topicArray) {

        String realm = topicArray[1];
        AssetFilter<?> assetFilter = new AssetFilter<>().setRealm(realm);
        assetFilter.setAssetIds(mqttEventHandler.getAssetId(topicArray[1]+"/"+topicArray[3]));
        Asset<?> asset =  assetService.find(MqttEventHandler.getAssetId(topicArray[1]+"/"+topicArray[3]));
        String[] attributeNamesArray = asset.getAttributes().values().stream()
                .map(attribute -> attribute.getName())
                .toArray(String[]::new);
        assetFilter.setAttributeNames(attributeNamesArray);

        return assetFilter;
    }

    protected static Map<String, Object> prepareHeaders(String requestRealm, RemotingConnection connection) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(SESSION_KEY, getConnectionIDString(connection));
        headers.put(HEADER_CONNECTION_TYPE, ClientEventService.HEADER_CONNECTION_TYPE_MQTT);
        headers.put(REALM_PARAM_NAME, requestRealm);
        headers.put(HEADER_SOURCE, CLIENT);
        return headers;
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
    public boolean checkCanSubscribe(RemotingConnection connection, KeycloakSecurityContext securityContext, Topic topic) {
        // Skip standard checks
        return canSubscribe(connection, securityContext, topic);
    }

}
