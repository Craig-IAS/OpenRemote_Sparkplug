package org.openremote.model.custom;

import org.openremote.model.asset.Asset;
import org.openremote.model.asset.AssetDescriptor;
import org.openremote.model.value.AttributeDescriptor;


import jakarta.persistence.Entity;
import org.openremote.model.value.ValueType;

import java.util.Optional;

@Entity
public class SparkplugAsset extends Asset<SparkplugAsset>   {

    public static final AttributeDescriptor<String> GROUP_ID = new AttributeDescriptor<>("GroupId", ValueType.TEXT);
    public static final AttributeDescriptor<String> DEVICE_ID = new AttributeDescriptor<>("DeviceId", ValueType.TEXT).withOptional(true);


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



}
