package org.logstashplugins;

import co.elastic.logstash.api.*;

import java.util.*;

/**
 * Created by: Safak T. at 5/4/2020
 * While listening: A Beautiful Song - ROZEN @Link https://open.spotify.com/track/6XxspOXHSwzodDVfBD9uKk
 */
@LogstashPlugin(name = "evam_filter_plugin")
public class EvamFilterPlugin implements Filter {
    
    private final String id;

    public static final PluginConfigSpec<Boolean> ONE_EVENT_PER_OUTPUT_ACTION =
            PluginConfigSpec.booleanSetting("oneEventPerOutputAction", false);

    public EvamFilterPlugin(String id, Configuration config, Context context) {
        this.id = id;
    }
    @Override
    public Collection<Event> filter(Collection<Event> collection, FilterMatchListener filterMatchListener) {
        List<Event> convertedEvents = new LinkedList<>();
        for (Event event : collection){
            Map<String,Object> dataMap = event.toMap();
            Object scenario_name = dataMap.get("scenarioName");
            Object actor_id = dataMap.get("actorId");
            
            Map<String, Object> transitionMap = (Map<String, Object>) dataMap.get("transition");
            Object previous_state = transitionMap.get("previousState");
            Object state = transitionMap.get("nextState");
            
            Map<String, Object> eventMap = (Map<String, Object>) dataMap.get("event");
            Object event_name = eventMap.get("name");
            Object event_id = eventMap.get("id");

            Object scenario_hist_id = dataMap.get("scenarioHistId");
            Object log_time = dataMap.get("date");
            
            StringBuilder event_param_text_builder = new StringBuilder();
            List<Map<String,Object>> eventParameters = (List<Map<String, Object>>) eventMap.get("parameters");
            eventParameters.forEach(parameter -> {
                Object paramName = parameter.get("name");
                Object paramValue = parameter.get("stringValue");
                event_param_text_builder.append("+");
                event_param_text_builder.append(paramName);
                event_param_text_builder.append("~");
                event_param_text_builder.append(paramValue);
                event_param_text_builder.append("~");
            });
            
            List<Map<String,Object>> eventAggregatedParameters = (List<Map<String, Object>>) eventMap.get("aggregatedParameters");
            eventAggregatedParameters.forEach(parameter -> {
                Object paramName = parameter.get("name");
                Object paramValue = parameter.get("stringValue");
                event_param_text_builder.append("+");
                event_param_text_builder.append(paramName);
                event_param_text_builder.append("~");
                event_param_text_builder.append(paramValue);
                event_param_text_builder.append("~");
            });

            List<Map<String,Object>> enrichments = (List<Map<String, Object>>) dataMap.get("enrichments");
            enrichments.forEach(parameter -> {
                Object paramName = parameter.get("name");
                Object paramValue = parameter.get("stringValue");
                event_param_text_builder.append("&");
                event_param_text_builder.append(paramName);
                event_param_text_builder.append("~");
                event_param_text_builder.append(paramValue);
                event_param_text_builder.append("~");
            });

            List<Map<String,Object>> actorVariables = (List<Map<String, Object>>) dataMap.get("actorVariables");
            actorVariables.forEach(parameter -> {
                Object paramName = parameter.get("name");
                Object paramValue = parameter.get("stringValue");
                event_param_text_builder.append("#");
                event_param_text_builder.append(paramName);
                event_param_text_builder.append("~");
                event_param_text_builder.append(paramValue);
                event_param_text_builder.append("~");
            });

            List<Map<String,Object>> globalVariables = (List<Map<String, Object>>) dataMap.get("globalVariables");
            globalVariables.forEach(parameter -> {
                Object paramName = parameter.get("name");
                Object paramValue = parameter.get("stringValue");
                event_param_text_builder.append("%");
                event_param_text_builder.append(paramName);
                event_param_text_builder.append("~");
                event_param_text_builder.append(paramValue);
                event_param_text_builder.append("~");
            });

            String event_param_text = event_param_text_builder.toString();
            
            StringBuilder validation_text_builder = new StringBuilder();
            List<Map<String,Object>> validations = (List<Map<String, Object>>) dataMap.get("validations");
            validations.forEach(parameter -> {
                Object validationName = parameter.get("name");
                Object validationResult = parameter.get("result");
                validation_text_builder.append(validationName);
                validation_text_builder.append("~");
                validation_text_builder.append(validationResult);
                validation_text_builder.append("~");
            });
            
            String validation_text = validation_text_builder.toString();

            List<Map<String,Object>> outputActions = (List<Map<String, Object>>) dataMap.get("outputActions");
            List<Event> newEvents = new LinkedList<>();
            if (!outputActions.isEmpty()){
                outputActions.forEach(outputAction -> {
                    StringBuilder eventParamTextBuilderPerOA = new StringBuilder(event_param_text);
                    Object oa_name = outputAction.get("name");
                    Object oa_template_name = outputAction.get("templateName");
                    Object oa_return_code = outputAction.get("returnCode");

                    List<Map<String,Object>> inputParameters = (List<Map<String, Object>>) outputAction.get("inputParameters");
                    inputParameters.forEach(inputParameter -> {
                        Object input_param_name = inputParameter.get("name");
                        Object input_param_val = inputParameter.get("stringValue");
                        eventParamTextBuilderPerOA.append(oa_name);
                        eventParamTextBuilderPerOA.append(".");
                        eventParamTextBuilderPerOA.append(input_param_name);
                        eventParamTextBuilderPerOA.append("~");
                        eventParamTextBuilderPerOA.append(input_param_val);
                        eventParamTextBuilderPerOA.append("~");
                    });

                    List<Map<String,Object>> returnParameters = (List<Map<String, Object>>) outputAction.get("returnParameters");
                    returnParameters.forEach(returnParameter -> {
                        Object return_param_name = returnParameter.get("name");
                        Object return_param_val = returnParameter.get("stringValue");
                        eventParamTextBuilderPerOA.append("$");
                        eventParamTextBuilderPerOA.append(oa_name);
                        eventParamTextBuilderPerOA.append(".");
                        eventParamTextBuilderPerOA.append(return_param_name);
                        eventParamTextBuilderPerOA.append("~");
                        eventParamTextBuilderPerOA.append(return_param_val);
                        eventParamTextBuilderPerOA.append("~");
                    });
                    
                    String eventParamText = eventParamTextBuilderPerOA.toString();
                    
                    Event newEvent = new org.logstash.Event();
                    newEvent.setField("[ACTORID]",actor_id);
                    newEvent.setField("[SCENARIO_NAME]",scenario_name);
                    newEvent.setField("[PREVIOUS_STATE]",previous_state);
                    newEvent.setField("[STATE]",state);
                    // burayı byte yapalım ----->
                    newEvent.setField("[EVENT_PARAMS]",eventParamText.substring(0, Math.min(eventParamText.length(), 999)));
                    // burayı byte yapalım ----->
                    newEvent.setField("[EVENT]",event_name);
                    newEvent.setField("[ACTION_NAME]",oa_name);
                    newEvent.setField("[ACTION_TEMPLATE_NAME]",oa_template_name);
                    newEvent.setField("[ACTION_RTCODE]",oa_return_code);
                    newEvent.setField("[LOG_TIME]",log_time);
                    newEvent.setField("[EVENT_ID]",event_id);
                    newEvent.setField("[VALIDATIONS]",validation_text);
                    newEvent.setField("[SCENARIO_HIST_ID]",scenario_hist_id);
                    newEvents.add(newEvent);
                });
            }else{
                Event newEvent = new org.logstash.Event();
                newEvent.setField("[ACTORID]",actor_id);
                newEvent.setField("[SCENARIO_NAME]",scenario_name);
                newEvent.setField("[PREVIOUS_STATE]",previous_state);
                newEvent.setField("[STATE]",state);
                newEvent.setField("[EVENT_PARAMS]",event_param_text.substring(0, Math.min(event_param_text.length(), 999)));
                newEvent.setField("[EVENT]",event_name);
                newEvent.setField("[LOG_TIME]",log_time);
                newEvent.setField("[EVENT_ID]",event_id);
                newEvent.setField("[VALIDATIONS]",validation_text);
                newEvent.setField("[SCENARIO_HIST_ID]",scenario_hist_id);
                newEvents.add(newEvent);
            }
            convertedEvents.addAll(newEvents);
        }
        return convertedEvents;
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return Collections.emptyList();
    }

    @Override
    public String getId() {
        return id;
    }
}
