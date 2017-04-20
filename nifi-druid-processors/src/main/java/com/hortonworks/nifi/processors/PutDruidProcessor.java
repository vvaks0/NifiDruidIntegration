package com.hortonworks.nifi.processors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.hortonworks.nifi.controller.api.DruidTranquilityService;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

@SideEffectFree
@Tags({"Druid","Timeseries","OLAP"})
@CapabilityDescription("Sends events to Apache Druid for Indexing")
public class PutDruidProcessor extends AbstractProcessor {
	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	//private FlowFile flowFile;

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
		//ProvenanceReporter provRep = session.getProvenanceReporter();
		
		DruidTranquilityService tranquilityController = context.getProperty(DRUID_TRANQUILITY_SERVICE).asControllerService(DruidTranquilityService.class);
		Tranquilizer<String> tranquilizer = tranquilityController.getTranquilizer();
		
        FlowFile flowFile = session.get();
        if (flowFile == null || flowFile.getSize() == 0) {
            return;
        }
		
		final ObjectMapper mapper = new ObjectMapper();
		final AtomicReference<JsonNode> rootNodeRef = new AtomicReference<>(null);
		try {
			session.read(flowFile, new InputStreamCallback() {
				@Override
				public void process(final InputStream in) throws IOException {
					try (final InputStream bufferedIn = new BufferedInputStream(in)) {
						rootNodeRef.set(mapper.readTree(bufferedIn.toString()));
						//String string = IOUtils.toString(bufferedIn).toString();
					}
				}
			});
		} catch (final ProcessException pe) {
			getLogger().error("Failed to parse {} due to {}; routing to failure", new Object[] {flowFile, pe.toString()}, pe);
			session.transfer(flowFile, REL_FAIL);
			return;
		}
		
		getLogger().debug(tranquilizer.status().toString());
		Future<?> future = tranquilizer.send(rootNodeRef.toString());
	    getLogger().debug("Sent Payload to Druid: " + rootNodeRef.toString());
	    
	    future.addEventListener(new FutureEventListener<Object>() {
	    	@Override
	    	public void onFailure(Throwable cause) {
	    		if (cause instanceof MessageDroppedException) {
	    			getLogger().error("FlowFile Dropped due to MessageDroppedException: " + cause);
	    			getLogger().error("Transfering FlowFIle to DROPPED relationship");
	    			//session.transfer(flowFile, REL_DROPPED);
	    		} else {
	    			getLogger().error("FlowFile Processing Failed due to: " + cause);
	    			getLogger().error("Transfering FlowFIle to FAIL relationship");
	    			//session.transfer(flowFile, REL_FAIL);
	    		}
	    	}

	    	@Override
	    	public void onSuccess(Object value) {
	    		getLogger().debug("FlowFile Processing Success : "+ value);
	    	}
	    });
		
		//flowFile = session.putAllAttributes(flowFile, (Map<String, String>) new ArrayList());
		session.transfer(flowFile, REL_SUCCESS);
	}
}