package com.hashmapinc.tempus.processors;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

@Tags({"OPC", "OPCUA", "UA"})
@CapabilityDescription("Fetches a response from an OPC UA server based on configured name space and input item names")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class GetOPCDataSubscription extends AbstractProcessor {

    public static final PropertyDescriptor OPC_ENDPOINT_URL = new PropertyDescriptor
            .Builder().name("OPC_ENDPOINT_URL")
            .description("OPC_ENDPOINT_URL")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successful OPC read")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed OPC read")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(OPC_ENDPOINT_URL);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private AtomicReference<OpcUaClient> opcUaClient = new AtomicReference<>();
    private final AtomicLong clientHandles = new AtomicLong(1L);
    private UaSubscription subscription = null;
    private final HashMap<String, UaMonitoredItem> items = new HashMap<>();

    private final ArrayList<Pair<UaMonitoredItem, DataValue>> buffer = new ArrayList<>(); // TODO Use external distributed buffer

    private OpcUaClient initClient(String endpointUrl) throws Exception {
        SecurityPolicy securityPolicy = SecurityPolicy.None;

        EndpointDescription[] endpoints = UaTcpStackClient.getEndpoints(endpointUrl).get();
        EndpointDescription endpoint = Arrays.stream(endpoints)
                .filter(e -> e.getSecurityPolicyUri().equals(securityPolicy.getSecurityPolicyUri()))
                .findFirst().orElseThrow(() -> new Exception("(((("));

        OpcUaClientConfig config = OpcUaClientConfig.builder()
                .setApplicationUri("urn:digitalpetri:opcua:client")
                .setEndpoint(endpoint)
                .setRequestTimeout(uint(10000))
                .build(); // TODO more settigns

        return new OpcUaClient(config);
    }


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();


        if (opcUaClient.get() == null) {
            try {
                opcUaClient.set(initClient(processContext.getProperty(OPC_ENDPOINT_URL).getValue()));
            } catch (Exception e) {
                logger.error("Error initializeing client", e);
                throw new ProcessException("Error initializing client", e);

            }
        }

        // Initialize  response variable
        final AtomicReference<Set<String>> requestedTagnames = new AtomicReference<>();

        // get FlowFile
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            logger.error("Flow File is null for session: " + session.toString());
            return;
        }
        // Read tag name from flow file content
        session.read(flowFile, in -> {
            try {
                Set<String> tagname = new BufferedReader(new InputStreamReader(in))
                        .lines().collect(Collectors.toSet());

                requestedTagnames.set(tagname);

            } catch (Exception e) {
                logger.error("Failed to read", e);
            }

        });
        try {

            createSubscription(processContext, logger, requestedTagnames.get()); // TODO separate into two different processors: one for updating subscription, one for flushing the buffer
        } catch (Exception e) {
            logger.error("Error creating subscription", e);
            throw new ProcessException("Error creating subscription", e);
        }

        flushBuffer(session, flowFile, logger);
    }

    private void flushBuffer(ProcessSession session, FlowFile flowFile, ComponentLog logger) {

        try {
            flowFile = session.write(flowFile, outputStream -> {
                synchronized (buffer) {

                    JSONArray jsonArray = new JSONArray();
                    buffer.forEach(pair -> {


                        Object value = pair.getValue().getValue().getValue(); //value
                        if (value == null) {
                            return;
                        }

                        String[] key = pair.getKey().getReadValueId().getNodeId().toParseableString().split("=");

                        Class clazz = value.getClass();
                        // Build JSON element and add to JSON Array
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("id", key[key.length - 1]);
                        jsonObject.put("ts", getTimeStamp(pair.getValue(), "SourceTimestamp", true, logger)); // TODO more settings
                        if (clazz.equals(Double.class) || clazz.equals(Short.class) || clazz.equals(Integer.class)) {
                            jsonObject.put("vd", value);
                        } else if (clazz.equals(Long.class) || clazz.equals(Float.class)) {
                            jsonObject.put("vd", value);
                        } else {
                            jsonObject.put("vs", value.toString().trim());
                        }
                        jsonObject.put("q", (Object) pair.getValue().getStatusCode().getValue());
                        jsonArray.put(jsonObject);
                    });
                    JSONObject finalJsonObject = new JSONObject()
                            .put("values", jsonArray);

                    outputStream.write(finalJsonObject.toString().getBytes());
                    buffer.clear();
                }
            });
            session.transfer(flowFile, SUCCESS);
        } catch (Exception e) {
            session.transfer(flowFile, FAILURE);
            logger.error("Error dumping buffer", e);
        }

    }

    private String getTimeStamp(DataValue value, String returnTimestamp, boolean longTimestamp, ComponentLog logger) throws RuntimeException {
        String ts = null;
        LocalDateTime ldt = null;
        DateTimeFormatter formatPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Get Timestamp
        try {

            if (returnTimestamp.equals("ServerTimestamp")) {
                if (longTimestamp) {
                    ts = value.getServerTime().getJavaTime() + ""; //TODO proper checks
                } else {

                    ts = Utils.convertStringDateFormat(value.getServerTime().toString(), "MM/dd/yy HH:mm:ss.SSSSSSS z", "yyyy-MM-dd HH:mm:ss.SSS");

                }
            }
            if (returnTimestamp.equals("SourceTimestamp")) {
                if (longTimestamp) {
                    ts = value.getSourceTime().getJavaTime() + "";
                } else {

                    ts = Utils.convertStringDateFormat(value.getSourceTime().toString(), "MM/dd/yy HH:mm:ss.SSSSSSS z", "yyyy-MM-dd HH:mm:ss.SSS");

                }
            }


        } catch (Exception ex) {
            logger.error("Error extracting ts", ex);
            throw ex;
        }
        return ts;
    }


    private void createSubscription(ProcessContext context, ComponentLog logger, Set<String> nodeIds) throws Exception {
        if (subscription == null) {
            subscription = opcUaClient.get().getSubscriptionManager()
                    .createSubscription(1000.0).get();
        }
        synchronized (items) {
            Sets.SetView<String> toAdd = Sets.difference(nodeIds, items.keySet());
            Sets.SetView<String> toRemove = Sets.difference(items.keySet(), nodeIds);


            List<UaMonitoredItem> removeList = toRemove.stream().map(items::get).collect(Collectors.toList());
            subscription.deleteMonitoredItems(removeList);
            toRemove.forEach(items::remove);

            try {
                // WAT
                Method parse = NodeId.class.getDeclaredMethod("parse", String.class); // TODO probably, it's due to classpath problems


                List<MonitoredItemCreateRequest> monitoredItemCreateRequests = new ArrayList<>();
                toAdd.forEach(nodeId -> {

                    NodeId node;
                    try {
                        // WAT
                        node = ((NodeId) parse.invoke(null, nodeId));
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        logger.error("Error executing parse method", e);
                        throw new RuntimeException(e);
                    }
                    ReadValueId readValueId =
                            new ReadValueId(
                                    node,
                                    AttributeId.Value.uid(),
                                    null,
                                    QualifiedName.NULL_VALUE
                            );

                    UInteger clientHandle = uint(clientHandles.getAndIncrement());

                    MonitoringParameters parameters = new MonitoringParameters(
                            clientHandle,
                            1000.0,     // sampling interval
                            null,       // filter, null means use default
                            uint(10),   // queue size
                            true        // discard oldest
                    );

                    MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(
                            readValueId, MonitoringMode.Reporting, parameters);

                    monitoredItemCreateRequests.add(request);
                });

                BiConsumer<UaMonitoredItem, Integer> onItemCreated =
                        (item, id) -> item.setValueConsumer(this::onSubscriptionValue);

                List<UaMonitoredItem> items = subscription.createMonitoredItems(
                        TimestampsToReturn.Both,
                        monitoredItemCreateRequests,
                        onItemCreated
                ).get();

                for (UaMonitoredItem item : items) {
                    if (item.getStatusCode().isGood()) {
                        logger.debug("item created for nodeId=" + item.getReadValueId().getNodeId());
                        this.items.put(item.getReadValueId().getNodeId().toParseableString(), item);
                    } else {
                        logger.error(item.getReadValueId().getNodeId().toString() + item.getStatusCode());
                    }
                }

            } catch (InterruptedException | ExecutionException ex) {
                logger.error("Error", ex);
            }
        }
    }

    private void onSubscriptionValue(UaMonitoredItem item, DataValue value) {
        ComponentLog logger = getLogger();
        logger.debug("subscription value received: item=" + item.getReadValueId().getNodeId() + ", value=" + value.getValue());
        try {
//            String fieldName = item.getReadValueId().getNodeId().getIdentifier().toString();
            synchronized (buffer) {
                buffer.add(Pair.of(item, value));
            }
//            process(ImmutableList.of(value), item.getReadValueId().getNodeId() + "." + counter.getAndIncrement(), fieldName);
        } catch (Exception ex) {
            logger.error("Error buffering value " + item + " " + value, ex);
//            LOG.error(Errors.OPC_UA_09.getMessage(), ex.getMessage(), ex);
//            errorQueue.offer(new StageException(Errors.OPC_UA_09, ex.getMessage(), ex));
        }
    }
}
