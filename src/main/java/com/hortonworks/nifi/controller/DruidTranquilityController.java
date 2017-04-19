package com.hortonworks.nifi.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.reporting.InitializationException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Period;

import com.hortonworks.nifi.controller.api.DruidTranquilityService;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeamConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidEnvironment;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.metamx.tranquility.typeclass.Timestamper;

import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;

@Tags({"Druid","Timeseries","OLAP"})
@CapabilityDescription("Provides a controller service to manage property files.")
public class DruidTranquilityController extends AbstractControllerService implements DruidTranquilityService{
	private String indexService = "druid/overlord"; // Your overlord's druid.service;
	private String discoveryPath = "/druid/discovery"; // Your overlord's druid.discovery.curator.path;
	private int clusterPartitions = 1;
    private int clusterReplication = 1 ;
    private String indexRetryPeriod = "PT10M";
    
    private Tranquilizer tranquilizer = null;
    
	static final PropertyDescriptor DATASOURCE = new PropertyDescriptor.Builder()
            .name("data_source")
            .description("Druid Data Source")
            .required(true)
            //.allowableValues("json", "xml")
            //.defaultValue("json")
            .build();
	
	static final PropertyDescriptor CONNECT_STRING = new PropertyDescriptor.Builder()
            .name("zk_connect_string")
            .description("ZK Connect String for Druid ")
            .required(true)
            //.allowableValues("json", "xml")
            //.defaultValue("json")
            .build();
	
	static final PropertyDescriptor TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
            .name("timestamp_field")
            .description("The name of the field that will be used as the timestamp. Should be in ISO format.")
            .required(true)
            //.allowableValues("json", "xml")
            .defaultValue("timestamp")
            .build();
	
	static final PropertyDescriptor AGGREGATOR_JSON = new PropertyDescriptor.Builder()
            .name("aggregators_descriptor")
            .description("Tranquility compliant JSON string that defines what aggregators to apply on ingest.")
            .required(true)
            //.allowableValues("json", "xml")
            //.defaultValue("json")
            .build();
	
	static final PropertyDescriptor DIMENSIONS_LIST = new PropertyDescriptor.Builder()
            .name("dimensions_list")
            .description("A comma separated list of field names that will be stored as dimensions on ingest.")
            .required(true)
            //.allowableValues("json", "xml")
            //.defaultValue("json")
            .build();
	
	static final PropertyDescriptor QUERY_GRANULARITY = new PropertyDescriptor.Builder()
            .name("query_granularity")
            .description("Time unit by which to group and aggregate/rollup events.")
            .required(true)
            .allowableValues("RAW","SECOND","MINUTE","HOUR","DAY","MONTH","YEAR")
            .defaultValue("MINUTE")
            .build();
	
	static final PropertyDescriptor WINDOW_PERIOD = new PropertyDescriptor.Builder()
            .name("window_period")
            .description("Grace period to allow late arriving events for real time ingest.")
            .required(true)
            .allowableValues("PT1M","PT10M","PT60M")
            .defaultValue("PT10M")
            .build();

	 private static final List<PropertyDescriptor> properties;
	
	static{
		final List<PropertyDescriptor> props = new ArrayList<>();
	    props.add(DATASOURCE);
	    props.add(CONNECT_STRING);
	    props.add(AGGREGATOR_JSON);
	    props.add(QUERY_GRANULARITY);
	    props.add(WINDOW_PERIOD);
	    props.add(TIMESTAMP_FIELD);
	    
	    properties = Collections.unmodifiableList(props);
	}
	
	@Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

	@OnEnabled
	public void onConfigured(final ConfigurationContext context) throws InitializationException{
		getLogger().info("Starting properties file service");
	   
		final String dataSource = context.getProperty(DATASOURCE).getValue();
		final String zkConnectString = context.getProperty(CONNECT_STRING).getValue();
		final String timestampField  = context.getProperty(TIMESTAMP_FIELD).getValue();
		final String queryGranularity = context.getProperty(QUERY_GRANULARITY).getValue();
		final String windowPeriod = context.getProperty(WINDOW_PERIOD).getValue();
		final String aggregatorJSON = context.getProperty(AGGREGATOR_JSON).getValue();
		final String dimensionsStringList = context.getProperty(DIMENSIONS_LIST).getValue();
		
		final List<String> dimensions = getDimensions(dimensionsStringList);
	    final List<AggregatorFactory> aggregator = getAggregatorList(aggregatorJSON);
	    
		final Timestamper<Map<String, Object>> timestamper = new Timestamper<Map<String, Object>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public DateTime timestamp(Map<String, Object> theMap){
				return new DateTime(theMap.get(timestampField));
			}
		};

		// Tranquility uses ZooKeeper (through Curator) for coordination.
		final CuratorFramework curator = CuratorFrameworkFactory
		                .builder()
		                .connectString(zkConnectString)
		                .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
		                .build();
		curator.start();

		// The JSON serialization of your object must have a timestamp field in a format that Druid understands. By default,
		// Druid expects the field to be called "timestamp" and to be an ISO8601 timestamp.
		final TimestampSpec timestampSpec = new TimestampSpec(timestampField, "auto", null);
		        
		//final TranquilityConfig<PropertiesBasedConfig> druidIngestConfig = TranquilityConfig.read(druidConfig);
		//final DataSourceConfig<PropertiesBasedConfig> druidDataSourceConfig = druidIngestConfig.getDataSource(dataSource);
		final Beam<Map<String, Object>> beam = DruidBeams.builder(timestamper)
			    		.curator(curator)
		                .discoveryPath(discoveryPath)
		                .location(DruidLocation.create(DruidEnvironment.create(indexService, dataSource), zkConnectString))
		                .timestampSpec(timestampSpec)
		                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregator, QueryGranularity.fromString(queryGranularity)))
		                .tuning(
		                        ClusteredBeamTuning
		                                .builder()
		                                //.segmentGranularity(Granularity.MINUTE)
		                                .segmentGranularity(getSegmentGranularity(queryGranularity))
		                                .windowPeriod(new Period(windowPeriod))
		                                .partitions(clusterPartitions)
		                                .replicants(clusterReplication)
		                                .build()
		                )
		                .druidBeamConfig(
		                        DruidBeamConfig
		                                .builder()
		                                .indexRetryPeriod(new Period(indexRetryPeriod))
		                                .build())
		                .buildBeam();
		
		tranquilizer = Tranquilizer.builder()
                .maxBatchSize(10000000)
                .maxPendingBatches(1000)
                .lingerMillis(1000)
                .blockOnFull(true)
                .build(beam);
		
        tranquilizer.start();
	}
	
	public Tranquilizer getTranquilizer(){
		return tranquilizer;
	}
	
	private  List<Map<String, Map<String, String>>> parseJsonString(String aggregatorJson) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(aggregatorJson, List.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception while parsing the aggregratorJson");
        }
    }
	
	private List<String> getDimensions(String dimensionStringList){
		List<String> dimensionList = new LinkedList(Arrays.asList(dimensionStringList.split(",")));
		return dimensionList;
	}
	
	private List<AggregatorFactory> getAggregatorList(String aggregatorJSON) {
        List<AggregatorFactory> aggregatorList = new LinkedList<>();
        List<Map<String, Map<String, String>>> aggregatorInfo = parseJsonString(aggregatorJSON);
        for (Map<String, Map<String, String>> aggregator : aggregatorInfo) {

            if (aggregator.containsKey("count")) {
                Map<String, String> map = aggregator.get("count");
                aggregatorList.add(getCountAggregator(map));
            }
            else if (aggregator.containsKey("doublesum")) {
                Map<String, String> map = aggregator.get("doublesum");
                aggregatorList.add(getDoubleSumAggregator(map));
            }
            else if (aggregator.containsKey("doublemax")) {
                Map<String, String> map = aggregator.get("doublemax");
                aggregatorList.add(getDoubleMaxAggregator(map));
            }
            else if (aggregator.containsKey("doublemin")) {
                Map<String, String> map = aggregator.get("doublemin");
                aggregatorList.add(getDoubleMinAggregator(map));
            }
            else if (aggregator.containsKey("longsum")) {
                Map<String, String> map = aggregator.get("longsum");
                aggregatorList.add(getLongSumAggregator(map));
            }
            else if (aggregator.containsKey("longmax")) {
                Map<String, String> map = aggregator.get("longmax");
                aggregatorList.add(getLongMaxAggregator(map));
            }
            else if (aggregator.containsKey("longmin")) {
                Map<String, String> map = aggregator.get("longmin");
                aggregatorList.add(getLongMinAggregator(map));
            }
        }

        return aggregatorList;
    }
	
	private AggregatorFactory getLongMinAggregator(Map<String, String> map) {
        return new LongMinAggregatorFactory(map.get("name"), map.get("fieldName"));
    }

    private AggregatorFactory getLongMaxAggregator(Map<String, String> map) {
        return new LongMaxAggregatorFactory(map.get("name"), map.get("fieldName"));
    }

    private AggregatorFactory getLongSumAggregator(Map<String, String> map) {
        return new LongSumAggregatorFactory(map.get("name"), map.get("fieldName"));
    }

    private AggregatorFactory getDoubleMinAggregator(Map<String, String> map) {
        return new DoubleMinAggregatorFactory(map.get("name"), map.get("fieldName"));
    }

    private AggregatorFactory getDoubleMaxAggregator(Map<String, String> map) {
        return new DoubleMaxAggregatorFactory(map.get("name"), map.get("fieldName"));
    }

    private AggregatorFactory getDoubleSumAggregator(Map<String, String> map) {
        return new DoubleSumAggregatorFactory(map.get("name"), map.get("fieldName"));
    }

    private AggregatorFactory getCountAggregator(Map<String, String> map) {
        return new CountAggregatorFactory(map.get("name"));
    }
    
    private Granularity getSegmentGranularity(String segmentGranularity) {
        Granularity granularity = Granularity.HOUR;

        switch (segmentGranularity) {
            case "SECOND":
                granularity = Granularity.SECOND;
                break;
            case "MINUTE":
                granularity = Granularity.MINUTE;
                break;
            case "FIVE_MINUTE":
                granularity = Granularity.FIVE_MINUTE;
                break;
            case "TEN_MINUTE":
                granularity = Granularity.TEN_MINUTE;
                break;
            case "FIFTEEN_MINUTE":
                granularity = Granularity.FIFTEEN_MINUTE;
                break;
            case "HOUR":
                granularity = Granularity.HOUR;
                break;
            case "SIX_HOUR":
                granularity = Granularity.SIX_HOUR;
                break;
            case "DAY":
                granularity = Granularity.DAY;
                break;
            case "WEEK":
                granularity = Granularity.WEEK;
                break;
            case "MONTH":
                granularity = Granularity.MONTH;
                break;
            case "YEAR":
                granularity = Granularity.YEAR;
                break;
            default:
                break;
        }
        return granularity;
    }
}
