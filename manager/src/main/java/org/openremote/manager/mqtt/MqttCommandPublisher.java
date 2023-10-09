package org.openremote.manager.mqtt;

import org.eclipse.tahu.host.CommandPublisher;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.Topic;
import org.eclipse.tahu.mqtt.MqttServerName;

public class MqttCommandPublisher implements CommandPublisher {

    @Override
    public void publishCommand(Topic topic, SparkplugBPayload sparkplugBPayload) throws Exception {

    }

    @Override
    public void publishCommand(MqttServerName mqttServerName, Topic topic,
            SparkplugBPayload sparkplugBPayload) throws Exception {

    }
}
