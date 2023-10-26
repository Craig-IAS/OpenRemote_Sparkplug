package org.openremote.manager.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import jdk.jfr.FlightRecorderPermission;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.eclipse.tahu.SparkplugInvalidTypeException;
import org.eclipse.tahu.SparkplugParsingException;
import org.eclipse.tahu.host.CommandPublisher;
import org.eclipse.tahu.host.manager.EdgeNodeManager;
import org.eclipse.tahu.host.manager.SparkplugEdgeNode;
import org.eclipse.tahu.host.model.HostApplicationMetricMap;
import org.eclipse.tahu.message.SparkplugBPayloadEncoder;
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
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload;
import org.openremote.container.message.MessageBrokerService;
import org.openremote.container.timer.TimerService;
import org.openremote.manager.asset.AssetStorageService;
import org.openremote.model.asset.Asset;
import org.openremote.model.attribute.Attribute;
import org.openremote.model.attribute.AttributeEvent;
import org.openremote.model.attribute.AttributeMap;
import org.openremote.model.attribute.MetaItem;
import org.openremote.model.attribute.MetaMap;
import org.openremote.model.custom.SparkplugAsset;
import org.openremote.model.syslog.SyslogCategory;
import org.openremote.model.value.MetaItemType;
import org.openremote.model.value.ValueDescriptor;
import org.openremote.model.value.ValueType;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import static org.openremote.model.syslog.SyslogCategory.API;

public class MqttCommandPublisher implements CommandPublisher {

    protected AssetStorageService assetService;
    protected TimerService timerService;
    protected MQTTBrokerService mqttBrokerService;

    protected  HostApplicationMetricMap hostApplicationMetricMap;
    protected  EdgeNodeManager edgeNodeManager;
    protected SparkplugBPayloadEncoder encoder;
    private static final Logger LOG = SyslogCategory.getLogger(API, MqttCommandPublisher.class);

    public MqttCommandPublisher(AssetStorageService assetService, TimerService timerService,
            MQTTBrokerService mqttBrokerService) {
        this.assetService = assetService;
        this.timerService = timerService;
        this.mqttBrokerService = mqttBrokerService;
        this.hostApplicationMetricMap = HostApplicationMetricMap.getInstance();
        this.edgeNodeManager = EdgeNodeManager.getInstance();
        this.encoder = new SparkplugBPayloadEncoder();
    }

    @Override
    public void publishCommand(Topic topic, SparkplugBPayload sparkplugBPayload) throws Exception {

    }

    public void publishCommandFromEvent(String[] topicArray, AttributeEvent attributeEvent, RemotingConnection connection)  throws Exception {
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



    public void processAttributeEvent(AttributeEvent attributeEvent) {


        MqttQoS mqttQoS = MqttQoS.AT_MOST_ONCE;
        Asset<?> asset = assetService.find(attributeEvent.getAssetId());
        if(!(asset instanceof SparkplugAsset))return;
        AttributeMap attributes = asset.getAttributes();
        Optional<Attribute<?>> optionalAttribute = attributes.get(attributeEvent.getAttributeName());
        Attribute<?> eventAttribute = optionalAttribute.get();
        MetaMap metaMap = eventAttribute.getMeta();
        Optional<MetaItem<?>> optionalMetaItem = metaMap.get("label");
        MetaItem<?> metaItem = optionalMetaItem.get();
        String label = String.valueOf(metaItem.getValue().get());
        EdgeNodeDescriptor edgeNodeDescriptor = new EdgeNodeDescriptor(
                (String) attributes.get("GroupId").get().getValue().get(),
                (String) attributes.get("DeviceId").get().getValue().get());
        SparkplugEdgeNode sparkplugEdgeNode = EdgeNodeManager.getInstance().getSparkplugEdgeNode(edgeNodeDescriptor);
        Date timestamp = new Date(attributeEvent.getTimestamp());
        SparkplugBPayloadBuilder CMD = new SparkplugBPayloadBuilder().setTimestamp(timestamp);
        SparkplugBPayload payload;
        Topic topic = new Topic(SparkplugConstants.NAMESPACE,
                edgeNodeDescriptor.getGroupId(),
                edgeNodeDescriptor.getEdgeNodeId(),
                MessageType.NCMD);
        if (sparkplugEdgeNode == null){
            try {
                LOG.fine("Unknown Edgenode requesting new birth message");
                Metric rebirth = new Metric(SparkplugConstants.REBIRTH,null,timestamp,MetricDataType.Boolean,false,false,null,null,true);
                CMD.addMetric(rebirth);
                payload = CMD.createPayload();
                mqttBrokerService.publishByteMessage(topic.toString(),encoder.getBytes(payload,false), mqttQoS);
            } catch (SparkplugInvalidTypeException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }else{
            Metric metric = sparkplugEdgeNode.getMetric(label);
            Object metricValue = metric.getValue();
            Object eventValue = castToType(metric.getDataType(),attributeEvent.getValue().get());
            //the value is the same as the cache, ignore to prevent infinite loop
            if(eventValue.equals(metricValue)) return;

            try {
                Metric metricToSend = new Metric(label,null,timestamp,metric.getDataType(),false,false,null,null,eventValue);
                CMD = new SparkplugBPayloadBuilder().setTimestamp(timestamp);
                CMD.addMetric(metricToSend);
                payload = CMD.createPayload();
                mqttBrokerService.publishByteMessage(topic.toString(),encoder.getBytes(payload,false), mqttQoS);
                sparkplugEdgeNode.updateValue(label,eventValue);
            } catch (SparkplugInvalidTypeException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }




        return;
    }
}
