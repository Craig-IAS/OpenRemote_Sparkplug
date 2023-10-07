package org.openremote.manager.mqtt;


import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.keycloak.KeycloakSecurityContext;

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
        return Set.of(
                "spBv1"+"/0/" + TOKEN_MULTI_LEVEL_WILDCARD
        );
    }

    @Override
    public void onPublish(RemotingConnection connection, Topic topic, ByteBuf body) {
        //log the topic and body
        getLogger().info("onPublish: " + topic + " " + body.toString());

    }

    @Override
    public boolean checkCanPublish(RemotingConnection connection, KeycloakSecurityContext securityContext, Topic topic) {
        // Skip standard checks
        return canPublish(connection, securityContext, topic);

    }

}
