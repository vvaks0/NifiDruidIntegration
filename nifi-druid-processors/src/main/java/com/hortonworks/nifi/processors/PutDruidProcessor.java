package com.hortonworks.nifi.processors;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.hortonworks.nifi.controller.api.DruidTranquilityService;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

import scala.runtime.BoxedUnit;

@SideEffectFree
@Tags({"Druid","Timeseries","OLAP"})
@CapabilityDescription("Sends events to Apache Druid for Indexing")
public class PutDruidProcessor
        extends AbstractProcessor {

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final PropertyDescriptor DRUID_TRANQUILITY_SERVICE = new PropertyDescriptor.Builder()
            .name("druid_tranquility_service")
            .description("Tranquility Service to use for sending events to Druid")
            .required(true)
            .identifiesControllerService(DruidTranquilityService.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();

    public static final Relationship REL_FAIL = new Relationship.Builder()
            .name("FAIL")
            .description("FlowFiles are routed to this relationship when they cannot be parsed")
            .build();

    public static final Relationship REL_DROPPED = new Relationship.Builder()
            .name("DROPPED")
            .description("FlowFiles are routed to this relationship when they are outside of the configured time window, timestamp format is invalid, ect...")
            .build();

    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DRUID_TRANQUILITY_SERVICE);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_DROPPED);
        relationships.add(REL_FAIL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {

        DruidTranquilityService tranquilityController = context.getProperty(DRUID_TRANQUILITY_SERVICE)
                .asControllerService(DruidTranquilityService.class);
        Tranquilizer<Map<String,Object>> tranquilizer = tranquilityController.getTranquilizer();

        final FlowFile flowFile = session.get();
        if (flowFile == null || flowFile.getSize() == 0) {
            return;
        }

        final byte[] buffer = new byte[(int) flowFile.getSize()];   // Dangerous! Flowfile size could be larger than machine architecture int!
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });

        String contentString = new String(buffer, StandardCharsets.UTF_8);  // Dangerous! Should be pulling the system default charset. Charset.defaultCharset()
        Map<String,Object> contentMap = null;
        try {
            contentMap = new ObjectMapper().readValue(contentString, HashMap.class);
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        getLogger().debug("********** Tranquilizer Status: " + tranquilizer.status().toString());
        Future<BoxedUnit> future = tranquilizer.send(contentMap);
        getLogger().debug("********** Sent Payload to Druid: " + contentMap);

        future.addEventListener(new FutureEventListener<Object>() {
            @Override
            public void onFailure(Throwable cause) {
                if (cause instanceof MessageDroppedException) {
                    getLogger().error("********** FlowFile Dropped due to MessageDroppedException: " + cause.getMessage() + " : " + cause);
                    cause.getStackTrace();
                    getLogger().error("********** Transfering FlowFile to DROPPED relationship");
                    session.transfer(flowFile, REL_DROPPED);
                } else {
                    getLogger().error("********** FlowFile Processing Failed due to: " + cause.getMessage() + " : " + cause);
                    cause.printStackTrace();
                    getLogger().error("********** Transfering FlowFile to FAIL relationship");
                    session.transfer(flowFile, REL_FAIL);
                }
            }

            @Override
            public void onSuccess(Object value) {
                getLogger().debug("********** FlowFile Processing Success : "+ value);
                session.transfer(flowFile, REL_SUCCESS);
            }
        });

        try {
            Await.result(future);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}