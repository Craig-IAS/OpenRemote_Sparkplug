package org.openremote.manager.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import jdk.jfr.FlightRecorderPermission;
import org.eclipse.tahu.SparkplugInvalidTypeException;
import org.eclipse.tahu.SparkplugParsingException;
import org.eclipse.tahu.host.CommandPublisher;
import org.eclipse.tahu.host.manager.EdgeNodeManager;
import org.eclipse.tahu.host.model.HostApplicationMetricMap;
import org.eclipse.tahu.message.model.EdgeNodeDescriptor;
import org.eclipse.tahu.message.model.MessageType;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugBPayload.SparkplugBPayloadBuilder;
import org.eclipse.tahu.message.model.SparkplugDescriptor;
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

import java.math.BigDecimal;
import java.util.Date;
import java.util.Optional;

public class MqttCommandPublisher implements CommandPublisher {

    protected AssetStorageService assetService;
    protected TimerService timerService;
    protected MQTTBrokerService mqttBrokerService;

    protected  HostApplicationMetricMap hostApplicationMetricMap;
    protected  EdgeNodeManager edgeNodeManager;

    public MqttCommandPublisher(AssetStorageService assetService, TimerService timerService,
            MQTTBrokerService mqttBrokerService) {
        this.assetService = assetService;
        this.timerService = timerService;
        this.mqttBrokerService = mqttBrokerService;
        this.hostApplicationMetricMap = HostApplicationMetricMap.getInstance();
        this.edgeNodeManager = EdgeNodeManager.getInstance();
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

        SparkplugDescriptor sparkplugDescriptor = topic.getSparkplugDescriptor();
        EdgeNodeDescriptor edgeNodeDescriptor =  topic.getEdgeNodeDescriptor();




        try{
            Optional<Attribute<?>> optionalAttribute = asset.getAttribute(atributeName);
            if (!optionalAttribute.isPresent()) return;
            Attribute<?> attribute = optionalAttribute.get();
            MetaMap meta = attribute.getMeta();
            Optional<MetaItem<?>> metaItem = meta.get("label");
            if (!metaItem.isPresent()) return;
            String label = (String) metaItem.get().getValue().get();
            //get the datatype from the nodeManger
            //cast the value to that class
            MetricDataType dt = hostApplicationMetricMap.getDataType(edgeNodeDescriptor,sparkplugDescriptor,label);
            Object castedValue = castToType(dt, attributeEvent.getValue().get());
            Metric metric = new Metric(label,null,attributeEvent.timestamp,dt,false,false,null,null,castedValue);

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

    public static Object castToType(MetricDataType metricDataType, Object value) {
        Class<?> dt = metricDataType.getClazz();
        if (dt.equals(Float.class)){
            return Float.parseFloat(value.toString());
        }
        return dt.cast(value);

    }



}
