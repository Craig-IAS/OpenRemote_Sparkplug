package org.openremote.manager.mqtt;

import org.eclipse.tahu.host.api.HostApplicationEventHandler;
import org.eclipse.tahu.message.model.DeviceDescriptor;
import org.eclipse.tahu.message.model.EdgeNodeDescriptor;
import org.eclipse.tahu.message.model.Message;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.SparkplugDescriptor;

public class MqttEventHandler implements  HostApplicationEventHandler {


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

    }

    @Override
    public void onStale(SparkplugDescriptor sparkplugDescriptor, Metric metric) {

    }
}
