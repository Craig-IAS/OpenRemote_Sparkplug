package org.openremote.model.event.shared;

import org.openremote.model.asset.Asset;
import org.openremote.model.attribute.AttributeEvent;

public class SparkplugEventFilter<T extends AttributeEvent> extends EventFilter<T>{



    @Override
    public String getFilterType() {
        return null;
    }

    @Override
    public T apply(T event) {



        return event;
    }
}
