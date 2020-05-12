package org.logstashplugins;

import co.elastic.logstash.api.DeadLetterQueueWriter;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.Plugin;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Created by: Safak T. at 5/11/2020
 * While listening: Misfits T-Shirt - DREAMERS @Link https://open.spotify.com/track/2ZkWAGwx0UDKQSD9lmNecF
 */
public class MockedDLQWriter implements DeadLetterQueueWriter {

    private Logger logger;
    
    public MockedDLQWriter(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void writeEntry(Event event, Plugin plugin, String s) throws IOException {
        logger.error("DLQ ENTRY ENTERED: {}", event.getData().toString());
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public long getCurrentQueueSize() {
        return 0;
    }
}
