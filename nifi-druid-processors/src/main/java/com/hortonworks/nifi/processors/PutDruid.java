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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamThrottler;
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
public class PutDruid extends AbstractSessionFactoryProcessor {

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final Map<Object,String> messageStatus = new HashMap<Object,String>();
    
    private final ConcurrentMap<String, FlowFileEntryTimeWrapper> flowFileMap = new ConcurrentHashMap<>();
    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();
    private final AtomicReference<StreamThrottler> throttlerRef = new AtomicReference<>();

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
    
    private Set<String> findOldFlowFileIds(final ProcessContext ctx) {
        final Set<String> old = new HashSet<>();

        //final long expiryMillis = ctx.getProperty(MAX_UNCONFIRMED_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        final long expiryMillis = 60000;
        final long cutoffTime = System.currentTimeMillis() - expiryMillis;
        for (final Map.Entry<String, FlowFileEntryTimeWrapper> entry : flowFileMap.entrySet()) {
            final FlowFileEntryTimeWrapper wrapper = entry.getValue();
            if (wrapper != null && wrapper.getEntryTime() < cutoffTime) {
                old.add(entry.getKey());
            }
        }
        return old;
    }
    
    private void processFlowFile(ProcessContext context, ProcessSession session){
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
         
         String[] messageArray = contentString.split("\\R");
         
         for(String message: messageArray){
         	try {
         		contentMap = new ObjectMapper().readValue(message, HashMap.class);
         		//contentMap = new ObjectMapper().readValue(message, HashMap.class);
         	} catch (JsonParseException e) {
         		e.printStackTrace();
         	} catch (JsonMappingException e) {
         		e.printStackTrace();
         	} catch (IOException e) {
         		e.printStackTrace();
         	}

         	getLogger().debug("********** Tranquilizer Status: " + tranquilizer.status().toString());
         	messageStatus.put(flowFile, "pending");
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
         			getLogger().debug("********** FlowFile Processing Success : "+ value.toString());
         			session.transfer(flowFile, REL_SUCCESS);
         		}
         	});

         	
         	try {
         		Await.result(future);
         	} catch (Exception e) {
         		e.printStackTrace();
         	}
         }	
         //session.transfer(flowFile, REL_SUCCESS);
         session.commit();
    }
    
    public void onTrigger(ProcessContext context, ProcessSessionFactory factory) throws ProcessException {
    	final ProcessSession session = factory.createSession();
    	
    	processFlowFile(context, session);
    }
    
    public static class FlowFileEntryTimeWrapper {

        private final Set<FlowFile> flowFiles;
        private final long entryTime;
        private final ProcessSession session;
        private final String clientIP;

        public FlowFileEntryTimeWrapper(final ProcessSession session, final Set<FlowFile> flowFiles, final long entryTime, final String clientIP) {
            this.flowFiles = flowFiles;
            this.entryTime = entryTime;
            this.session = session;
            this.clientIP = clientIP;
        }

        public Set<FlowFile> getFlowFiles() {
            return flowFiles;
        }

        public long getEntryTime() {
            return entryTime;
        }

        public ProcessSession getSession() {
            return session;
        }

        public String getClientIP() {
            return clientIP;
        }
    }
}