package org.openremote.manager.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.eclipse.tahu.SparkplugParsingException;
import org.eclipse.tahu.host.CommandPublisher;
import org.eclipse.tahu.message.model.MessageType;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugBPayload.SparkplugBPayloadBuilder;
import org.eclipse.tahu.message.model.Topic;
import org.eclipse.tahu.model.MetricDataTypeMap;
import org.eclipse.tahu.mqtt.MqttServerName;
import org.openremote.container.message.MessageBrokerService;
import org.openremote.container.timer.TimerService;
import org.openremote.manager.asset.AssetStorageService;
import org.openremote.model.asset.Asset;
import org.openremote.model.attribute.Attribute;
import org.openremote.model.attribute.AttributeEvent;
import org.openremote.model.attribute.MetaItem;
import org.openremote.model.attribute.MetaMap;
import org.openremote.model.custom.SparkplugAsset;
import org.openremote.model.value.MetaItemType;
import org.openremote.model.value.ValueDescriptor;
import org.openremote.model.value.ValueType;

import java.util.Date;
import java.util.Optional;

public class MqttCommandPublisher implements CommandPublisher {

    protected AssetStorageService assetService;
    protected TimerService timerService;
    protected MQTTBrokerService mqttBrokerService;

    public MqttCommandPublisher(AssetStorageService assetService, TimerService timerService,
            MQTTBrokerService mqttBrokerService) {
        this.assetService = assetService;
        this.timerService = timerService;
        this.mqttBrokerService = mqttBrokerService;
    }

    @Override
    public void publishCommand(Topic topic, SparkplugBPayload sparkplugBPayload) throws Exception {

    }

    public void publishCommandFromEvent(String[] topicArray, AttributeEvent attributeEvent)  throws Exception {
        Topic topic = getTopicFromArray(topicArray);
        String assetId = MqttEventHandler.getAssetId(getAssitIdFromTopic(topic));
        String atributeName = attributeEvent.getAttributeName();
        Asset<?> asset = assetService.find(assetId);
        MqttQoS mqttQoS = MqttQoS.AT_MOST_ONCE;
        try{
            Optional<Attribute<?>> optionalAttribute = asset.getAttribute(atributeName);
            if (!optionalAttribute.isPresent()) return;
            Attribute<?> attribute = optionalAttribute.get();
            MetaMap meta = attribute.getMeta();
            Optional<MetaItem<?>> metaItem = meta.get("label");
            if (!metaItem.isPresent()) return;
            String label = (String) metaItem.get().getValue().get();
            int dt = mapValueDescriptorToDataTypeIndex(attribute.getType());
            Metric metric = new Metric(label,null,attributeEvent.timestamp,MetricDataType.fromInteger(dt),false,false,null,null,attributeEvent.getValue().get());

            SparkplugBPayloadBuilder CMD = new SparkplugBPayloadBuilder().setTimestamp(attributeEvent.timestamp);
            CMD.addMetric(metric);
            mqttBrokerService.publishMessage(String.join("/", topicArray),CMD, mqttQoS);

           } catch (IllegalArgumentException e) {

        }





    }

    @Override
    public void publishCommand(MqttServerName mqttServerName, Topic topic,
            SparkplugBPayload sparkplugBPayload) throws Exception {

    }


    public static Topic getTopicFromArray(String[] topicArray)
            throws SparkplugParsingException {
                if (topicArray.length == 4) {
            Topic tahuTopic = new Topic(topicArray[0], topicArray[1], topicArray[3], MessageType.NCMD);
            return tahuTopic;
        } else if (topicArray.length == 5) {
            Topic tahuTopic = new Topic(topicArray[0], topicArray[1], topicArray[3], topicArray[4], MessageType.DCMD);
            return tahuTopic;
        } else {
            throw new IllegalArgumentException("Invalid topic length: " + topicArray.length);
        }
    }
    public static String getAssitIdFromTopic(Topic topic) {
        return topic.getDeviceId() == null ? topic.getGroupId()+"/"+topic.getEdgeNodeId() :
                topic.getGroupId()+"/"+topic.getEdgeNodeId()+"/"+topic.getDeviceId();

    }

    public static int mapValueDescriptorToDataTypeIndex(ValueDescriptor<?> descriptor) {
        String type = descriptor.getName();

        switch (type) {
            case "integer":
                // You might need further logic here since INTEGER maps to multiple indices (1, 2, 3, 5, 6)
                // For this example, I'll just map it to one of them.
                return 1;  // Int8 (or others based on your actual requirement)

            case "long":
                // Similar to INTEGER, LONG maps to multiple indices (4, 7, 10).
                return 4;  // Int64 (or others based on your actual requirement)

            case "bigInteger":
                return 8;  // UInt64

            case "bigNumber":
                return 9;  // Float

            case "boolean":
                return 11;  // Boolean

            case "text":
                // TEXT maps to multiple indices (12, 14). You'd need to distinguish them based on further logic.
                return 12;  // String

            case "dateAndTime":
                return 13;  // DateTime

            case "UUID":
                return 15;  // UUID

            case "JSON":
                return 16;  // DataSet

            case "byte":
                return 17;  // Bytes

            // ... continue the mapping for other ValueTypes ...

            default:
                throw new IllegalArgumentException("Unsupported ValueDescriptor: " + descriptor);
        }
    }

}
