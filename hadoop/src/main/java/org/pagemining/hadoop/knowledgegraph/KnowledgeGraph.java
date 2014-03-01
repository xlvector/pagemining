package org.pagemining.hadoop.knowledgegraph;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KnowledgeGraph {
    private JSONObject definition;
    private Map<String, Set<String>> propertyBelongedTypes = new HashMap<String, Set<String>>();
    private Map<String, Integer> propertyCount = new HashMap<String, Integer>();
    private Map<String, Set<String>> propertyDefinedTypes = new HashMap<String, Set<String>>();

    public KnowledgeGraph(String def){
        definition = (JSONObject) JSONValue.parse(def);
    }

    private void buildIndex(){
        for(Map.Entry<String, Object> e : definition.entrySet()){
            String type = e.getKey();
            if(e.getValue() instanceof JSONObject){
                JSONObject def = (JSONObject)e.getValue();
                propertyCount.put(type, def.size());
                for(Map.Entry<String, Object> pt : def.entrySet()){
                    String property = pt.getKey();
                    String definedType = (String)pt.getValue();
                    if(!propertyBelongedTypes.containsKey(property)){
                        propertyBelongedTypes.put(property, new HashSet<String>());
                    }
                    propertyBelongedTypes.get(property).add(type);
                    if(!propertyDefinedTypes.containsKey(definedType)){
                        propertyDefinedTypes.put(property, new HashSet<String>());
                    }
                    propertyDefinedTypes.get(property).add(definedType);
                }
            }
        }
    }

    private int propertyCount(String type){
        if(propertyCount.containsKey(type)){
            return propertyCount.get(type);
        }
        return 0;
    }

    private String guessType(JSONObject obj){
        Map<String, Integer> rank = new HashMap<String, Integer>();
        for(Map.Entry<String, Object> e : obj.entrySet()){
            Set<String> belongedTypes = propertyBelongedTypes.get(e.getKey());
            if(belongedTypes != null){
                for(String type : belongedTypes){
                    if(!rank.containsKey(type)){
                        rank.put(type, 0);
                    }
                    rank.put(type, 1 + rank.get(type));
                }
            }
        }
        String bestMatchType = null;
        double maxWeight = 0.0;
        for(Map.Entry<String, Integer> e : rank.entrySet()){
            double weight = e.getValue().doubleValue() / (double)propertyCount(e.getKey());
            if(weight < maxWeight){
                maxWeight = weight;
                bestMatchType = e.getKey();
            }
        }
        return bestMatchType;
    }
}
