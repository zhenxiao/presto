/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.uber.data.presto.eventlistener;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import hpw_shaded.com.uber.data.heatpipe.HeatpipeFactory;
import hpw_shaded.com.uber.data.heatpipe.configuration.Heatpipe4JConfig;
import hpw_shaded.com.uber.data.heatpipe.configuration.PropertiesHeatpipeConfiguration;
import hpw_shaded.com.uber.data.heatpipe.errors.HeatpipeEncodeError;
import hpw_shaded.com.uber.data.heatpipe.errors.SchemaServiceNotAvailableException;
import hpw_shaded.com.uber.data.heatpipe.producer.HeatpipeProducer;
import hpw_shaded.com.uber.data.heatpipe.producer.HeatpipeProducerFactory;
import hpw_shaded.com.uber.stream.java.kafka.rest.client.KafkaRestClientException;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class UberEventListener
        implements EventListener
{
    private static final Logger log = Logger.get(UberEventListener.class);
    private final String engine;
    private final String cluster;
    private final HeatpipeProducer producer;

    public UberEventListener(String topic, String engine, String cluster, boolean syncProduce)
    {
        this.engine = engine;
        this.cluster = cluster;

        Properties prop = new Properties();
        prop.setProperty("heatpipe.app_id", "PrestoEventListener");
        prop.setProperty("kafka.syncProduction", Boolean.toString(syncProduce));
        Heatpipe4JConfig heatpipe4JConfig = new PropertiesHeatpipeConfiguration(prop);

        this.producer = createHeatpipeProducer(topic, heatpipe4JConfig);
    }

    private HeatpipeProducer createHeatpipeProducer(String topic, Heatpipe4JConfig config)
    {
        try {
            HeatpipeFactory heatpipeFactory = new HeatpipeFactory(config);
            HeatpipeProducerFactory heatpipeProducerFactory = new HeatpipeProducerFactory(config);
            Integer version = Collections.max(heatpipeFactory.getClient().getSchemaVersions(topic));
            return heatpipeProducerFactory.get(topic, version);
        }
        catch (IOException | SchemaServiceNotAvailableException | KafkaRestClientException ex) {
            log.error("Failed to create kafka producer" + ex);
            ex.printStackTrace();
            return null;
        }
    }

    void sendToHeatpipe(QueryEventInfo queryEventInfo)
    {
        if (producer == null) {
            return;
        }

        try {
            producer.produce(queryEventInfo.toMap());
        }
        catch (HeatpipeEncodeError heatpipeEncodeError) {
            log.error("failed to send data: " + heatpipeEncodeError);
            heatpipeEncodeError.printStackTrace();
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        QueryEventInfo queryEventInfo = new QueryEventInfo(queryCreatedEvent, this.engine, this.cluster);
        sendToHeatpipe(queryEventInfo);
        log.info("Query created. query id: " + queryEventInfo.getQueryId()
                + ", state: " + queryEventInfo.getState());
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        QueryEventInfo queryEventInfo = new QueryEventInfo(queryCompletedEvent, this.engine, this.cluster);
        sendToHeatpipe(queryEventInfo);
        log.info("Query completed. query id: " + queryEventInfo.getQueryId()
                + ", state: " + queryEventInfo.getState());
    }
}
