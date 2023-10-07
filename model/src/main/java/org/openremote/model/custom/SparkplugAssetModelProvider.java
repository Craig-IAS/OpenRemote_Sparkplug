package org.openremote.model.custom;

import org.openremote.model.AssetModelProvider;
import org.openremote.model.asset.Asset;
import org.openremote.model.asset.AssetDescriptor;
import org.openremote.model.value.AttributeDescriptor;
import org.openremote.model.value.MetaItemDescriptor;
import org.openremote.model.value.ValueDescriptor;

import java.util.List;
import java.util.Map;

public class SparkplugAssetModelProvider implements AssetModelProvider {


    @Override
    public boolean useAutoScan() {
        return true;
    }

    @Override
    public AssetDescriptor<?>[] getAssetDescriptors() {
        return new AssetDescriptor[0];
    }

    @Override
    public Map<Class<? extends Asset<?>>, List<AttributeDescriptor<?>>> getAttributeDescriptors() {
        return null;
    }

    @Override
    public Map<Class<? extends Asset<?>>, List<MetaItemDescriptor<?>>> getMetaItemDescriptors() {
        return null;
    }

    @Override
    public Map<Class<? extends Asset<?>>, List<ValueDescriptor<?>>> getValueDescriptors() {
        return null;
    }

    @Override
    public void onAssetModelFinished() {

    }
}
