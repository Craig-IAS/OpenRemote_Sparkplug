package org.openremote.model.custom;

import org.openremote.model.asset.Asset;
import org.openremote.model.asset.AssetDescriptor;
import org.openremote.model.attribute.MetaItem;
import org.openremote.model.attribute.MetaMap;
import org.openremote.model.value.AttributeDescriptor;


import jakarta.persistence.Entity;
import org.openremote.model.value.MetaItemType;
import org.openremote.model.value.ValueFormat;
import org.openremote.model.value.ValueType;

import java.math.BigDecimal;
import java.util.Optional;

@Entity
public class SparkplugAsset extends Asset<SparkplugAsset>   {

    public static final AttributeDescriptor<String> GROUP_ID = new AttributeDescriptor<>("GroupId", ValueType.TEXT);
    public static final AttributeDescriptor<String> DEVICE_ID = new AttributeDescriptor<>("DeviceId", ValueType.TEXT);
    public static final AttributeDescriptor<Boolean> ONLINE = new AttributeDescriptor<>("Online", ValueType.BOOLEAN).withFormat(ValueFormat.BOOLEAN_ON_OFF());
    public static final AttributeDescriptor<Boolean> NODE_CONTROL_REBOOT = new AttributeDescriptor<>("NodeControlReboot", ValueType.BOOLEAN).withFormat(ValueFormat.BOOLEAN_AS_PRESSED_RELEASED());
    public static final AttributeDescriptor<Boolean> NODE_CONTROL_REBIRTH = new AttributeDescriptor<>("NodeControlRebirth", ValueType.BOOLEAN).withFormat(ValueFormat.BOOLEAN_AS_PRESSED_RELEASED());
    public static final AttributeDescriptor<Boolean> NODE_CONTROL_NEXT_SERVER= new AttributeDescriptor<>("NodeControlNextServer", ValueType.BOOLEAN).withFormat(ValueFormat.BOOLEAN_AS_PRESSED_RELEASED());






    public static final AssetDescriptor<SparkplugAsset> DESCRIPTOR = new AssetDescriptor<>("sparkplug", null, SparkplugAsset.class);

    /**
     * For use by hydrators (i.e. JPA/Jackson)
     */
    protected SparkplugAsset() {

    }

    public SparkplugAsset(String name) {
        super(name);
    }

    public Optional<String> getGroupId() {
        return getAttributes().getValue(GROUP_ID);
    }

    public Optional<String> getDeviceId() {
        return getAttributes().getValue(DEVICE_ID);
    }

    public Optional<Boolean> getOnline() {
        return getAttributes().getValue(ONLINE);
    }

    public Optional<Boolean> getNodeControlReboot() {
        return getAttributes().getValue(NODE_CONTROL_REBOOT);
    }

    public Optional<Boolean> getNodeControlRebirth() {
        return getAttributes().getValue(NODE_CONTROL_REBIRTH);
    }

    public Optional<Boolean> getNodeControlNextServer() {
        return getAttributes().getValue(NODE_CONTROL_NEXT_SERVER);
    }




}
