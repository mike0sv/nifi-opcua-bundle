package com.hashmapinc.tempus.processors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by himanshu on 11/10/17.
 */
public class ConvertToJsonOPC  extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successful Json conversion.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed Json conversion.")
            .build();

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile1 = session.get();
        session.read(flowFile1, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                    JSONArray jsonArray = new JSONArray();
                    List<String> list = new BufferedReader(new InputStreamReader(in))
                            .lines().collect(Collectors.toList());
                    String[] arrayNameSpace = list.get(0).split(",");
                    Map<String,String> nsTagListMap = new HashMap<>();

                    for (int i = 1; i < list.size() ; i++ ) {
                        for (String nameSpace : arrayNameSpace) {
                            if(list.get(i).contains(nameSpace)){
                                String[] tagNames = list.get(i).split("\\.");
                                String str = nsTagListMap.get(nameSpace) + tagNames[tagNames.length - 1] + ",";
                                nsTagListMap.put(nameSpace,str);
                            }
                        }
                    }

                    for (Map.Entry<String,String> entry : nsTagListMap.entrySet()) {
                        String tagStr = entry.getValue();
                        String[] tags = tagStr.substring(0,tagStr.length() - 1).split(",");
                        for (int i = 0; i < tags.length ; i++){
                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put("device",entry.getKey());
                            String tbKey = "tag" + i;
                            jsonObject.put("key",tbKey);
                            jsonObject.put("value",tags[i]);
                            jsonArray.put(jsonObject);
                        }
                        //jsonObject.put("tags",tags.substring(0,tags.length() - 1));

                    }
                    FlowFile flowFile = session.create();

                    if ( flowFile != null ) {
                        try{
                            flowFile = session.write(flowFile, new OutputStreamCallback() {
                                public void process(OutputStream out) throws IOException {
                                    out.write(jsonArray.toString().getBytes());
                                }
                            });
                            session.transfer(flowFile, SUCCESS);
                        }catch (Exception e){
                            session.transfer(flowFile, FAILURE);
                        }
                    }
                    else {
                        session.transfer(flowFile, FAILURE);
                    }
                }catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed to read json string.");
                }
            }
        });
        session.transfer(flowFile1,FAILURE);
    }
}
