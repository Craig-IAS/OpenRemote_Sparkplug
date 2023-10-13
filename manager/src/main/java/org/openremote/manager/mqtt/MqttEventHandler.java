package org.openremote.manager.mqtt;

import org.eclipse.tahu.host.api.HostApplicationEventHandler;
import org.eclipse.tahu.host.manager.EdgeNodeManager;
import org.eclipse.tahu.host.manager.SparkplugEdgeNode;
import org.eclipse.tahu.host.model.HostApplicationMetricMap;
import org.eclipse.tahu.host.model.HostMetric;
import org.eclipse.tahu.message.model.DeviceDescriptor;
import org.eclipse.tahu.message.model.EdgeNodeDescriptor;
import org.eclipse.tahu.message.model.Message;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.PropertySet;
import org.eclipse.tahu.message.model.SparkplugDescriptor;
import org.openremote.container.message.MessageBrokerService;
import org.openremote.container.util.UniqueIdentifierGenerator;
import org.openremote.manager.asset.AssetStorageService;
import org.openremote.manager.event.ClientEventService;
import org.openremote.model.asset.Asset;
import org.openremote.model.attribute.Attribute;
import org.openremote.model.attribute.AttributeEvent;
import org.openremote.model.attribute.MetaItem;
import org.openremote.model.custom.SparkplugAsset;
import org.openremote.model.syslog.SyslogCategory;
import org.openremote.model.value.MetaItemType;
import org.openremote.model.value.ValueDescriptor;
import org.openremote.model.value.ValueType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.openremote.manager.event.ClientEventService.CLIENT_INBOUND_QUEUE;
import static org.openremote.manager.event.ClientEventService.HEADER_CONNECTION_TYPE;
import static org.openremote.manager.mqtt.DefaultMQTTHandler.prepareHeaders;
import static org.openremote.manager.mqtt.MQTTBrokerService.getConnectionIDString;
import static org.openremote.model.Constants.REALM_PARAM_NAME;
import static org.openremote.model.Constants.SESSION_KEY;
import static org.openremote.model.attribute.AttributeEvent.HEADER_SOURCE;
import static org.openremote.model.attribute.AttributeEvent.Source.CLIENT;
import static org.openremote.model.attribute.AttributeEvent.Source.SENSOR;
import static org.openremote.model.syslog.SyslogCategory.API;

public class MqttEventHandler implements  HostApplicationEventHandler {

    protected HostApplicationMetricMap  hostApplicationMetricMap;
    protected EdgeNodeManager edgeNodeManager;
    protected AssetStorageService assetStorageService;
    protected MessageBrokerService messageBrokerService;
    private static final Logger LOG = SyslogCategory.getLogger(API, MqttEventHandler.class);

    public MqttEventHandler(AssetStorageService assetStorageService, MessageBrokerService messageBrokerService ){
        this.hostApplicationMetricMap = HostApplicationMetricMap.getInstance();
        this.edgeNodeManager = EdgeNodeManager.getInstance();
        this.assetStorageService = assetStorageService;
        this.messageBrokerService = messageBrokerService;

    }

    @Override
    public void onConnect() {

    }

    @Override
    public void onDisconnect() {

    }

    @Override
    public void onMessage(SparkplugDescriptor sparkplugDescriptor, Message message) {

    }

    @Override
    public void onNodeBirthArrived(EdgeNodeDescriptor edgeNodeDescriptor, Message message) {

    }

    @Override
    public void onNodeBirthComplete(EdgeNodeDescriptor edgeNodeDescriptor) {

        SparkplugEdgeNode sparkplugEdgeNode = edgeNodeManager.getSparkplugEdgeNode(edgeNodeDescriptor);

        //find the asset
        String deviceUuid = getAssetId(edgeNodeDescriptor.getDescriptorString());
        Asset<?> asset = assetStorageService.find(deviceUuid);
        //build attribute list

        if (asset == null) CreateNewAsset(edgeNodeDescriptor);
        else{
            List<Attribute> attributesList = getAttributesFromPayload(sparkplugEdgeNode);
            asset.addOrReplaceAttributes(attributesList.toArray(new Attribute[attributesList.size()]));
            Asset<?> mergedAsset = assetStorageService.merge(asset);
            if(mergedAsset != null){
                LOG.info("Updated Asset with new birth message " + asset);
                //            getLogger().info()
            }else{
                LOG.info("Failed to update Asset: " + asset);
            }
        }




    }



    @Override
    public void onNodeDataArrived(EdgeNodeDescriptor edgeNodeDescriptor, Message message) {


    }

    @Override
    public void onNodeDataComplete(EdgeNodeDescriptor edgeNodeDescriptor) {




    }



    @Override
    public void onNodeDeath(EdgeNodeDescriptor edgeNodeDescriptor, Message message) {

    }

    @Override
    public void onNodeDeathComplete(EdgeNodeDescriptor edgeNodeDescriptor) {

    }

    @Override
    public void onDeviceBirthArrived(DeviceDescriptor deviceDescriptor, Message message) {

    }

    @Override
    public void onDeviceBirthComplete(DeviceDescriptor deviceDescriptor) {

    }

    @Override
    public void onDeviceDataArrived(DeviceDescriptor deviceDescriptor, Message message) {

    }

    @Override
    public void onDeviceDataComplete(DeviceDescriptor deviceDescriptor) {

    }

    @Override
    public void onDeviceDeath(DeviceDescriptor deviceDescriptor, Message message) {

    }

    @Override
    public void onDeviceDeathComplete(DeviceDescriptor deviceDescriptor) {

    }

    @Override
    public void onBirthMetric(SparkplugDescriptor sparkplugDescriptor, Metric metric) {

    }

    @Override
    public void onDataMetric(SparkplugDescriptor sparkplugDescriptor, Metric metric) {
        if (metric.getName().equals("bdSeq")){ return; }
        String deviceUuid = getAssetId(sparkplugDescriptor.getDescriptorString());
        AttributeEvent attributeEvent;
        if (metric.getTimestamp()!=null) attributeEvent =  new AttributeEvent(deviceUuid, formatMetricName(metric.getName()), metric.getValue(),metric.getTimestamp().getTime());
        else attributeEvent = new AttributeEvent(deviceUuid, formatMetricName(metric.getName()), metric.getValue());
        Map<String, Object> headers = new HashMap<>();
        headers.put(HEADER_SOURCE, SENSOR);
        messageBrokerService.getFluentProducerTemplate()
                .withHeaders(headers)
                .withBody(attributeEvent)
                .to(CLIENT_INBOUND_QUEUE)
                .asyncSend();

    }

    public static String getAssetId(String dscriptorString){
        return UniqueIdentifierGenerator.generateId(dscriptorString);
    }

    @Override
    public void onStale(SparkplugDescriptor sparkplugDescriptor, Metric metric) {

    }
    private void CreateNewAsset(EdgeNodeDescriptor edgeNodeDescriptor) {
        SparkplugEdgeNode sparkplugEdgeNode = edgeNodeManager.getSparkplugEdgeNode(edgeNodeDescriptor);
        SparkplugAsset sparkplugAsset = new SparkplugAsset(sparkplugEdgeNode.getEdgeNodeId())
                .setRealm(sparkplugEdgeNode.getGroupId())
                .setId(getAssetId(edgeNodeDescriptor.getDescriptorString()));
        List<Attribute> attributesList = getAttributesFromPayload(sparkplugEdgeNode);
        attributesList.add(new Attribute<>("GroupId", ValueType.TEXT, sparkplugEdgeNode.getGroupId()));
        attributesList.add(new Attribute<>("DeviceId", ValueType.TEXT, sparkplugEdgeNode.getEdgeNodeId()));
        attributesList.add(new Attribute<>("Online", ValueType.BOOLEAN, sparkplugEdgeNode.isOnline()).addMeta(new MetaItem(MetaItemType.READ_ONLY, "true")));



        sparkplugAsset.addOrReplaceAttributes(attributesList.toArray(new Attribute[attributesList.size()]));
        Asset<?> mergedAsset = assetStorageService.merge(sparkplugAsset);
        if(mergedAsset != null){
            LOG.info("Created new Sparkplug asset " + sparkplugAsset);
            //            getLogger().info()
        }else{
            LOG.info("Failed to Created Asset: " + sparkplugAsset);
        }


    }

    private List<Attribute> getAttributesFromPayload(SparkplugEdgeNode sparkplugEdgeNode) {
        List<Attribute> attributesList = new ArrayList<>();
        Map<String, HostMetric> metricMap = sparkplugEdgeNode.getMetricMap();

        //loop through the metricmap and create attributes for each metric
        for (Map.Entry<String, HostMetric> entry : metricMap.entrySet()) {
            HostMetric metric = entry.getValue();
            if (metric.getName().equals("bdSeq")){ continue ;}
            ValueDescriptor valueDescriptor = getValueDesciptor(metric.getDataType().toIntValue());
            Attribute attribute = new Attribute( formatMetricName(metric.getName()),valueDescriptor,metric.getValue()).addMeta(new MetaItem(MetaItemType.LABEL,metric.getName()));
            attribute.addMeta(new MetaItem(MetaItemType.STORE_DATA_POINTS,"true"));
            PropertySet propertySet =  metric.getProperties();
            if (propertySet != null && propertySet.containsKey("engUnit")) {
                String[]  units= propertySet.get("engUnit").getValue().toString().split(" ");
                MetaItem unit = new MetaItem(MetaItemType.UNITS,units);
                attribute.addMeta(unit);
            }

            attributesList.add(attribute);



        }
        return attributesList;

    }

    public static String formatMetricName(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        String[] words = input.split("[^a-zA-Z0-9]+"); // split by non-word characters
        StringBuilder camelCase = new StringBuilder();

        for (String word : words) {
            if (!word.isEmpty()) {
                // Capitalize the first letter and append to the result
                camelCase.append(Character.toUpperCase(word.charAt(0)))
                        .append(word.substring(1));
            }
        }

        return camelCase.toString();
    }

    /**
     * Returns the {@link ValueDescriptor} for the given {sparkplug data type.
     *
     * @param int index of the data type
     * @return {@link ValueDescriptor} for the given {@link Attribute} type
     */
    public static ValueDescriptor<?> getValueDesciptor(int index) {
        switch(index) {
            case 1:
                return ValueType.BYTE;  // Int8
            case 2:
                return new ValueDescriptor<>("Short", Short.class);  // Int16
            case 3:
                return ValueType.INTEGER;  // Int32
            case 4:
                return ValueType.LONG;     // Int64
            case 5:
                return new ValueDescriptor<>("Short", Short.class);  // UInt8
            case 6:
                return ValueType.INTEGER;  // UInt16
            case 7:
                return ValueType.LONG;     // UInt32
            case 8:
                return ValueType.BIG_INTEGER;  // UInt64
            case 9:
                return new ValueDescriptor<>("Float", Float.class);   // Float
            case 10:
                return ValueType.NUMBER ;   // Double
            case 11:
                return ValueType.BOOLEAN;  // Boolean
            case 12:
                return ValueType.TEXT;     // String
            case 13:
                return ValueType.DATE_AND_TIME;  // DateTime
            case 14:
                return ValueType.TEXT;     // Text
            case 15:
                return ValueType.UUID;     // UUID
            case 16:
                throw new IllegalArgumentException("Unsupported DataType index: " + index);  // DataSet.class TODO: implement this at another stage
            case 17:
                throw new IllegalArgumentException("Unsupported DataType index: " + index);
            // ... continue the mapping for other indices ...
            default:
                throw new IllegalArgumentException("Unsupported DataType index: " + index);
        }
    }




}
