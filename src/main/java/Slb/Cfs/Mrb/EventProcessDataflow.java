package Slb.Cfs.Mrb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.reflect.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.datastore.DatastoreV1.CommitRequest;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.LookupRequest;
import com.google.api.services.datastore.DatastoreV1.LookupResponse;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.api.services.datastore.client.DatastoreOptions;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.WithTimestamps;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterAll;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.CalendarWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.RetryHttpRequestInitializer;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;


public class EventProcessDataflow {
	static final Duration ONE_MINUTES = Duration.standardMinutes(1);
	static final Duration TWO_MINUTES = Duration.standardMinutes(2);
	static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
	static final Duration TEN_MINUTES = Duration.standardMinutes(10);
	static final Duration THIRTY_MINUTES = Duration.standardMinutes(30);
	static final Duration SIXTY_MINUTES = Duration.standardMinutes(60);

	private static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";
	private static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

	/**
	 * Class to hold info for a billing record
	 */
	@DefaultCoder(AvroCoder.class)
	static class BillingRecord
	{
		@Nullable String billingCycleId;
		@Nullable String subscriptionId;
		@Nullable String eventName;
		@Nullable String workflowName;
		@Nullable double cost;
		@Nullable double unitCost;
		@Nullable ArrayList<String> ids;
		@Nullable String idsString;

		public BillingRecord() {}
		public BillingRecord(String bid, String sid, String ename, String wname, double cost, Iterable<String> ids)
		{
			this.billingCycleId = bid;
			this.subscriptionId = sid;
			this.eventName = ename;
			this.workflowName = wname;
			this.ids = new ArrayList<String>();
			this.idsString = "";
			Iterator<String> iter = ids.iterator();
			while (iter.hasNext())
			{
				String id = iter.next();
				this.ids.add(id);
				this.idsString += id;
				if (iter.hasNext())
					this.idsString += ",";
			}
			this.cost = cost * this.ids.size();
			this.unitCost = cost;
		}
		public String getBillingCycleId() {
			return this.billingCycleId;
		}
		public String getEventName() {
			return this.eventName;
		}
		public String getWorkflowName() {
			return this.workflowName;
		}
		public String getSubscriptionId() {
			return this.subscriptionId;
		}
		public double getCost() {
			return this.cost;
		}
		public String getIds() {
			return this.idsString;
		}
		public void appendEvents(Iterable<String> ids) {
			Iterator<String> iter = ids.iterator();
			while (iter.hasNext())
			{
				String id = iter.next();
				if (id.length() > 0) {
					this.ids.add(id);
					this.idsString += ("," + id);
				}
			}
			this.cost = this.unitCost * this.ids.size();
		}

	}
	/**
	 * Class to hold info about a metered event.
	 */
	@DefaultCoder(AvroCoder.class)
	static class MeteredEvent {
		@Nullable String id;
		@Nullable String eventName;
		@Nullable String workflowName;
		@Nullable String userName;
		@Nullable String subscriptionId;
		@Nullable String correlationId;
		@Nullable Long timestamp;

		public MeteredEvent() {}

		public MeteredEvent(String id, String ename, String wname, String uname, String sid, String cid, Long timestamp) {
			this.id = id;
			this.eventName = ename;
			this.workflowName = wname;
			this.userName = uname;
			this.subscriptionId = sid;
			this.correlationId = cid;
			this.timestamp = timestamp;
		}

		public String getId() {
			return this.id;
		}
		public String getEventName() {
			return this.eventName;
		}
		public String getWorkflowName() {
			return this.workflowName;
		}
		public String getUserName() {
			return this.userName;
		}
		public String getSubscriptionId() {
			return this.subscriptionId;
		}
		public String getCorrelationId() {
			return this.correlationId;
		}
		public Long getTimestamp() {
			return this.timestamp;
		}
	}


	static interface Options extends PipelineOptions, DataflowPipelineOptions {

		@Description("Pub/Sub topic to read from")
		@Default.String("projects/anigbogu-sandbox1/topics/EventTest")
		String getTopic();
		void setTopic(String value);

		@Description("Prefix used for the BigQuery table names")
		@Default.String("dataflow_test")
		String getTableName();
		void setTableName(String value);

		@Description("BigQuery Dataset to write tables to. Must already exist.")
		@Default.String("billing_record_synthia")
		String getDataset();
		void setDataset(String value);

		@Description("Whether to keep jobs running on the Dataflow service after local process exit")
		@Default.Boolean(false)
		boolean getKeepJobsRunning();
		void setKeepJobsRunning(boolean keepJobsRunning);
	}


	/**
	 * Parses the raw game event info into MeteredEvent objects. Each event line has the following format:
	 * id,event_name,workflow_name,user_name,subscriptionn_id,correlation_id,timestamp_in_ms,readable_time
	 * The human-readable time string is not used here.
	 */
	static class ParseEventFn extends DoFn<String, MeteredEvent> {

		// Log and count parse errors.
		private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);
		private final Aggregator<Long, Long> numParseErrors =
				createAggregator("ParseErrors", new Sum.SumLongFn());

		@Override
		public void processElement(ProcessContext c) {
			String[] components = c.element().split(",");
			try {
				String id = components[0].trim();
				String ename = components[1].trim();
				String wname = components[2].trim();
				String uname = components[3].trim();
				String sid = components[4].trim();
				String cid = components[5].trim();
				Long timestamp = Long.parseLong(components[6].trim());
				MeteredEvent gInfo = new MeteredEvent(id, ename, wname, uname, sid, cid, timestamp);
				c.output(gInfo);
			} catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
				numParseErrors.addValue(1L);
				LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
			}
		}
	}
	
	static class FilterBillableEvent implements SerializableFunction<MeteredEvent, Boolean> {
		private ArrayList<String> billables;
		
		FilterBillableEvent(ArrayList<String> blist) {
			billables = new ArrayList<String>();
			billables.addAll(blist);
		}
		
		@Override
		public Boolean apply(MeteredEvent event) {
			String name = event.getEventName();
			if (billables.contains(name))
				return true;
			return false;
		}
	}

	static class UseEventTimeAsTimestamp implements SerializableFunction<MeteredEvent, Instant>	{
		@Override
		public Instant apply(MeteredEvent event) {
			return new Instant(event.timestamp);
		}
	}	
	

	/**
	 * A transform to extract key/event information from MeteredEvent.
	 * Then events are grouped by key and cost calculated.
	 * Finally, BillingRecordEntity is upsert into the datastore using the key.
	 * key: billingCycleId,subscriptionId,eventName,workflowName
	 */
	static class ExtractAndCalculateCost extends PTransform<PCollection<MeteredEvent>, PCollection<BillingRecord>> {
		@Override
		public PCollection<BillingRecord> apply(PCollection<MeteredEvent> mevents) {
			PCollection<KV<String, String>> binfo = mevents.apply(ParDo.of(new ExtractInfoFn()));
			PCollection<KV<String, Iterable<String>>> binfoGrouped = binfo.apply(GroupByKey.<String, String>create());
			PCollection<BillingRecord> brecords = binfoGrouped.apply(ParDo.of(new CalculateCostFn()));
			return brecords;
		}
	}

	static class ExtractInfoFn extends DoFn<MeteredEvent, KV<String, String>> 
		implements com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess {
		@Override
		public void processElement(ProcessContext c) {
			MeteredEvent mevent = c.element();
			IntervalWindow w = (IntervalWindow) c.window();
		    String wid = fmt.print(w.start());
			String bid = EventProcessDataflowHelper.GetBillingCycleIdForEvent(
					mevent.getSubscriptionId(), mevent.getEventName(), mevent.getWorkflowName(), mevent.getTimestamp());
			bid = wid + bid;
			String bkey = bid + "," + mevent.getSubscriptionId() + "," + mevent.getEventName() + "," + mevent.getWorkflowName();
			String bvalue = mevent.getId();
			c.output(KV.of(bkey,  bvalue));
		}
	}

	static class CalculateCostFn extends DoFn<KV<String, Iterable<String>>, BillingRecord>
	{
		private static final Logger LOG = LoggerFactory.getLogger(CalculateCostFn.class);
		@Override
		public void processElement(ProcessContext c) throws IOException, DatastoreException {
			KV<String, Iterable<String>> binfo = c.element();
			String[] components = binfo.getKey().split(",");
			double cost = EventProcessDataflowHelper.CalculateCostForEventGroup(components[0], components[1], components[2]);
			BillingRecord record = new BillingRecord(components[0], components[1], components[2], components[3], cost, binfo.getValue());
			String msg = WriteBillingRecordToDatastore(binfo.getKey(), record);
			LOG.info("WriteBillingRecordToDatastore: " + msg);
			c.output(record);
		}
	}

	static String WriteBillingRecordToDatastore(String skey, BillingRecord record) throws IOException, DatastoreException
	{
		String msg = "Current value [" + record.getIds() + "]";
		DatastoreOptions.Builder builder =
				new DatastoreOptions.Builder().dataset("anigbogu-sandbox1").initializer(
						new RetryHttpRequestInitializer());
		GoogleCredential credential = GoogleCredential.getApplicationDefault();
		if (credential != null) {
			builder.credential(credential);
		}
		Datastore ds = DatastoreFactory.get().create(builder.build());

		/*LookupRequest.Builder lookup = LookupRequest.newBuilder();
		lookup.addKey(DatastoreHelper.makeKey("BillingRecordEntity", skey).build());
		LookupResponse response = ds.lookup(lookup.build());
		if (response.getFoundCount() > 0) {
			Entity old = response.getFound(0).getEntity();
			Iterator<com.google.api.services.datastore.DatastoreV1.Property> piter = old.getPropertyList().iterator();
			while (piter.hasNext()) {
				com.google.api.services.datastore.DatastoreV1.Property ppt = piter.next();
				if (ppt.getName().equals("ids")) {
					String ids = ppt.getValue().getStringValue();
					msg += "Found value [" + ids + "]";
					String[] components = ids.split(",");
					List<String> aids = Arrays.asList(components);
					record.appendEvents(aids);	
					break;
				}
			}
		}*/

		Entity.Builder entb = Entity.newBuilder();
		entb.setKey(DatastoreHelper.makeKey("BillingRecordEntity", skey).build());
		entb.addProperty(DatastoreHelper.makeProperty("billingCycleId", DatastoreHelper.makeValue(record.getBillingCycleId())));
		entb.addProperty(DatastoreHelper.makeProperty("subscriptionId", DatastoreHelper.makeValue(record.getSubscriptionId())));
		entb.addProperty(DatastoreHelper.makeProperty("eventName", DatastoreHelper.makeValue(record.getEventName())));
		entb.addProperty(DatastoreHelper.makeProperty("workflowName", DatastoreHelper.makeValue(record.getWorkflowName())));
		entb.addProperty(DatastoreHelper.makeProperty("cost", DatastoreHelper.makeValue(record.getCost())));
		entb.addProperty(DatastoreHelper.makeProperty("ids", DatastoreHelper.makeValue(record.getIds())));

		CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
		commitRequest.getMutationBuilder().addUpsert(entb);
		commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
		ds.commit(commitRequest.build());
		return msg;
	}

	/**
	 * Create a map of information that describes how to write BillingRecord to BigQuery.
	 */
	protected static Map<String, WriteToBigQuery.FieldInfo<BillingRecord>> configureBillingRecordTableWrite() 
	{
		Map<String, WriteToBigQuery.FieldInfo<BillingRecord>> tableConfigure = new HashMap<String, WriteToBigQuery.FieldInfo<BillingRecord>>();
		tableConfigure.put("SubscriptionId",
				new WriteToBigQuery.FieldInfo<BillingRecord>("STRING", new SerializableFunction<DoFn<BillingRecord, TableRow>.ProcessContext, Object>() {
					@Override
					public Object apply(DoFn<BillingRecord, TableRow>.ProcessContext c) {
						return c.element().getSubscriptionId();
					}
				}));
		tableConfigure.put("EventName",
				new WriteToBigQuery.FieldInfo<BillingRecord>("STRING", new SerializableFunction<DoFn<BillingRecord, TableRow>.ProcessContext, Object>() {
					@Override
					public Object apply(DoFn<BillingRecord, TableRow>.ProcessContext c) {
						return c.element().getEventName();
					}
				}));
		tableConfigure.put("WorkflowName",
				new WriteToBigQuery.FieldInfo<BillingRecord>("STRING", new SerializableFunction<DoFn<BillingRecord, TableRow>.ProcessContext, Object>() {
					@Override
					public Object apply(DoFn<BillingRecord, TableRow>.ProcessContext c) {
						return c.element().getWorkflowName();
					}
				}));
		tableConfigure.put("Cost",
				new WriteToBigQuery.FieldInfo<BillingRecord>("FLOAT", new SerializableFunction<DoFn<BillingRecord, TableRow>.ProcessContext, Object>() {
					@Override
					public Object apply(DoFn<BillingRecord, TableRow>.ProcessContext c) {
						return c.element().getCost();
					}
				}));
		tableConfigure.put("Ids",
				new WriteToBigQuery.FieldInfo<BillingRecord>("STRING", new SerializableFunction<DoFn<BillingRecord, TableRow>.ProcessContext, Object>() {
					@Override
					public Object apply(DoFn<BillingRecord, TableRow>.ProcessContext c) {
						return c.element().getIds();
					}
				}));
		return tableConfigure;
	}
	protected static Map<String, WriteToBigQueryWindowed.FieldInfo<BillingRecord>> configureBillingRecordWindowTableWrite() 
	{
		Map<String, WriteToBigQueryWindowed.FieldInfo<BillingRecord>> tableConfigure = new HashMap<String, WriteToBigQueryWindowed.FieldInfo<BillingRecord>>();
		tableConfigure.put("SubscriptionId",
				new WriteToBigQueryWindowed.FieldInfo<BillingRecord>("STRING", new SerializableFunction<DoFn<BillingRecord, TableRow>.ProcessContext, Object>() {
					@Override
					public Object apply(DoFn<BillingRecord, TableRow>.ProcessContext c) {
						return c.element().getSubscriptionId();
					}
				}));
		tableConfigure.put("EventName",
				new WriteToBigQueryWindowed.FieldInfo<BillingRecord>("STRING", new SerializableFunction<DoFn<BillingRecord, TableRow>.ProcessContext, Object>() {
					@Override
					public Object apply(DoFn<BillingRecord, TableRow>.ProcessContext c) {
						return c.element().getEventName();
					}
				}));
		tableConfigure.put("WorkflowName",
				new WriteToBigQueryWindowed.FieldInfo<BillingRecord>("STRING", new SerializableFunction<DoFn<BillingRecord, TableRow>.ProcessContext, Object>() {
					@Override
					public Object apply(DoFn<BillingRecord, TableRow>.ProcessContext c) {
						return c.element().getWorkflowName();
					}
				}));
		tableConfigure.put("Cost",
				new WriteToBigQueryWindowed.FieldInfo<BillingRecord>("FLOAT", new SerializableFunction<DoFn<BillingRecord, TableRow>.ProcessContext, Object>() {
					@Override
					public Object apply(DoFn<BillingRecord, TableRow>.ProcessContext c) {
						return c.element().getCost();
					}
				}));
		tableConfigure.put("Ids",
				new WriteToBigQueryWindowed.FieldInfo<BillingRecord>("STRING", new SerializableFunction<DoFn<BillingRecord, TableRow>.ProcessContext, Object>() {
					@Override
					public Object apply(DoFn<BillingRecord, TableRow>.ProcessContext c) {
						return c.element().getIds();
					}
				}));
		tableConfigure.put("WindowStart",
	            new WriteToBigQueryWindowed.FieldInfo<BillingRecord>("STRING", new SerializableFunction<DoFn<BillingRecord, TableRow>.ProcessContext, Object>() {
	    			@Override
	    			public Object apply(DoFn<BillingRecord, TableRow>.ProcessContext c) { 
	    				IntervalWindow w = (IntervalWindow) c.window();
	    			    return fmt.print(w.start()); }
	    		}));
		return tableConfigure;
	}
	/**
	 * Create a map of information that describes how to write MeteredEvent to BigQuery.
	 */
	protected static Map<String, WriteToBigQuery.FieldInfo<MeteredEvent>> configureMeteredEventTableWrite() {
		Map<String, WriteToBigQuery.FieldInfo<MeteredEvent>> tableConfigure = new HashMap<String, WriteToBigQuery.FieldInfo<MeteredEvent>>();
		tableConfigure.put("Id", new WriteToBigQuery.FieldInfo<MeteredEvent>("STRING",
				new SerializableFunction<DoFn<MeteredEvent, TableRow>.ProcessContext, Object>() {
			@Override
			public Object apply(DoFn<MeteredEvent, TableRow>.ProcessContext c) {
				return c.element().getId();
			}
		}));
		tableConfigure.put("EventName", new WriteToBigQuery.FieldInfo<MeteredEvent>("STRING",
				new SerializableFunction<DoFn<MeteredEvent, TableRow>.ProcessContext, Object>() {
			@Override
			public Object apply(DoFn<MeteredEvent, TableRow>.ProcessContext c) {
				return c.element().getEventName();
			}
		}));
		tableConfigure.put("WorkflowName", new WriteToBigQuery.FieldInfo<MeteredEvent>("STRING",
				new SerializableFunction<DoFn<MeteredEvent, TableRow>.ProcessContext, Object>() {
			@Override
			public Object apply(DoFn<MeteredEvent, TableRow>.ProcessContext c) {
				return c.element().getWorkflowName();
			}
		}));
		tableConfigure.put("UserName", new WriteToBigQuery.FieldInfo<MeteredEvent>("STRING",
				new SerializableFunction<DoFn<MeteredEvent, TableRow>.ProcessContext, Object>() {
			@Override
			public Object apply(DoFn<MeteredEvent, TableRow>.ProcessContext c) {
				return c.element().getUserName();
			}
		}));
		tableConfigure.put("SubscriptionId", new WriteToBigQuery.FieldInfo<MeteredEvent>("STRING",
				new SerializableFunction<DoFn<MeteredEvent, TableRow>.ProcessContext, Object>() {
			@Override
			public Object apply(DoFn<MeteredEvent, TableRow>.ProcessContext c) {
				return c.element().getSubscriptionId();
			}
		}));
		tableConfigure.put("CorrelationId", new WriteToBigQuery.FieldInfo<MeteredEvent>("STRING",
				new SerializableFunction<DoFn<MeteredEvent, TableRow>.ProcessContext, Object>() {
			@Override
			public Object apply(DoFn<MeteredEvent, TableRow>.ProcessContext c) {
				return c.element().getCorrelationId();
			}
		}));
		tableConfigure.put("Timestamp", new WriteToBigQuery.FieldInfo<MeteredEvent>("STRING",
				new SerializableFunction<DoFn<MeteredEvent, TableRow>.ProcessContext, Object>() {
			@Override
			public Object apply(DoFn<MeteredEvent, TableRow>.ProcessContext c) {
				return c.element().getTimestamp().toString();
			}
		}));
		return tableConfigure;
	}
	public static void main(String[] args) throws Exception {

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		options.setStreaming(true);
		Pipeline pipeline = Pipeline.create(options);

		// Read events from Pub/Sub
		PCollection<MeteredEvent> mEvents = pipeline
				.apply(PubsubIO.Read.timestampLabel(TIMESTAMP_ATTRIBUTE).topic(options.getTopic()))
				.apply(ParDo.named("ParseIncomingEvent").of(new ParseEventFn()));

		// output MeteredEvent to BigQuery
		mEvents.apply("WriteMeteredEvent", new WriteToBigQuery<MeteredEvent>(
				options.getTableName() + "_MeteredEvent", configureMeteredEventTableWrite()));

		// calculate cost and output BillingRecord to Datastore
		/*mEvents.apply(Window.named("BillRecordFixedWindow").<MeteredEvent> into(FixedWindows.of(TWO_MINUTES)))
				.apply("CalculateCost", new ExtractAndCalculateCost())
				.apply("WriteBillingRecord", new WriteToBigQuery<BillingRecord>(
						options.getTableName() + "_BillingRecord", configureBillingRecordTableWrite()));*/
		
		// calculate cost, output to Datastore, and generate monthly invoce
		// fixed window should be a month, AllowedLateness should be a few days
		ArrayList<String> blist = EventProcessDataflowHelper.GetBillableEventList();
		mEvents
				.apply("FilterEventName", Filter.byPredicate(new FilterBillableEvent(blist)))
				.apply("UseEventTimeAsTimestamp", WithTimestamps.of(new UseEventTimeAsTimestamp()))
				//.apply(Window.named("MonthlyInvoiceWindow").<MeteredEvent> into(CalendarWindows.months(1))
				.apply(Window.named("InvoiceFixedWindow").<MeteredEvent> into(FixedWindows.of(TEN_MINUTES))
				.triggering(
						AfterWatermark.pastEndOfWindow()
						.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(TWO_MINUTES))
						.withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(TWO_MINUTES)))
				.withAllowedLateness(TEN_MINUTES).accumulatingFiredPanes())
				.apply("CalculateCost", new ExtractAndCalculateCost());
				/*.apply("WriteInvoice", new WriteToBigQueryWindowed<BillingRecord>(
						options.getTableName() + "_Invoice", configureBillingRecordWindowTableWrite()));*/


		PipelineResult result = pipeline.run();
	}
}
