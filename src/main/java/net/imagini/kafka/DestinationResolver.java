package net.imagini.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.gridport.kafka.MirrorDestination;
import co.gridport.kafka.MirrorResolver;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;


/**
 * This class is responsible for sending the incoming
 * events into the right topic and with a partition key
 * appropriate for partitioning stratgy assigned to
 * the meaning of each topic.
 * 
 * It uses low-level jackson json streaming API to
 * extract the deciding attributes of each event as
 * fast as possible.
 * 
 * @author: michal.haris@visualdna.com
 */

public class DestinationResolver  implements MirrorResolver
{

    private final Meter input = Metrics.newMeter(new MetricName(CentralMirror.group, CentralMirror.type,"input"), "messages", TimeUnit.SECONDS);
    private final Meter output = Metrics.newMeter(new MetricName(CentralMirror.group,CentralMirror.type,"output"), "messages", TimeUnit.SECONDS);
        
    static private Logger log = LoggerFactory.getLogger(DestinationResolver.class);
    static private JsonFactory jsonFactory = new JsonFactory();

    /**
     * Entry point method which decides how to resolve the message depending
     * on the source topic.
     */
    public List<MirrorDestination> resolve(MessageAndMetadata<Message> metaMsg)
    {
        ArrayList<MirrorDestination> result = new ArrayList<MirrorDestination>();

        //prepare fields we're interetsed in for parititoning purpose
        Map<String,String> fields = new HashMap<String,String>();
        fields.put("timestamp", null);
        fields.put("date", null);
        fields.put("event_type", null);
        fields.put("userUid", null);
        fields.put("sessionId", null);

        fields.put("action", null);
        fields.put("objType", null);
        fields.put("objId", null);
        fields.put("vdna_widget_mc", null);
        fields.put("partner_user_id", null);

        String json = null;
        try {
            json = parseMinimumJsonMessage(metaMsg.message(), fields);
        } catch (Exception e) {
            log.error("Couldn't read JSON ", e);
            return result;
        }

        String uuid = fields.get("userUid");
        String widget_mc = fields.get("vdna_widget_mc");
        String partner_user_id = fields.get("partner_user_id");
        String sessionId = fields.get("sessionId");
        Integer uidHash = null; 
        try {
            //figure out User UUID and its hash for partitioning
            if (uuid == null || uuid.equals("null") || uuid.equals("OPT_OUT") || uuid.equals("0"))
            {
                uuid = null;
                //TODO JIRA/EDA-19 what is userUid=OPT_OUT
                if (widget_mc != null && !widget_mc.equals("null") && !widget_mc.equals("OPT_OUT"))
                {
                    //TODO JIRA/EDA-19 validate the if this comes from the deterministic generator or a true uuid
                    uuid = widget_mc;
                }
            }
            if (uuid != null && !uuid.equals("null") && !uuid.equals("0"))
            {
                try {
                    uidHash = (uuid == null ? null : Math.abs(UUID.fromString(uuid).hashCode()));
                } catch (IllegalArgumentException invalidUuid) {
                    uidHash = null;
                }
            } else if (partner_user_id != null) {
                uidHash = Math.abs(partner_user_id.hashCode());
            } else if (sessionId != null) {
                uidHash = Math.abs(sessionId.hashCode());
            }
        } catch (Exception e3) {
            log.error("Error while trying to determine uuidHash of the event ", e3);
        }

        try {
            //now check event type and resolve accordingly
            String eventType = fields.get("event_type");
            if (eventType.equals("esVDNAAppUserActionEvent")) // event tracker message
            {
                String action = fields.get("action");
                if (action == null)
                {
                    log.warn("Invalid esVDNAAppUserActionEvent with null action: " + json);
                    return result;
                }
                /*
                 * Publish into one of the topics for each event type - e.g. primary topic.
                 * Every event has to end up at least in one of the following.
                 */
                if (action.startsWith("INSERTED_INTO_"))
                {
                    result.add(new MirrorDestination(
                        "datasync", 
                        uidHash
                    ));
                }
                else if (action.equals("CONVERSION") && fields.get("objId").equals("sync"))
                {
                    result.add(new MirrorDestination(
                        "datasync", 
                        uidHash
                    ));
                }
                else if (action.equals("MINTED_USER_KEY"))
                {
                    result.add(new MirrorDestination(
                        "datasync", 
                        uidHash
                    ));
                }
                else if (action.equals("CONVERSION") && fields.get("objType").equals("CONVERSION"))
                {
                    String conversionId = fields.get("objId");
                    if (conversionId.equals("loaded_quiz")
                        || conversionId.equals("started_quiz")
                        || conversionId.equals("loaded_results")
                    ) {
                        result.add(new MirrorDestination(
                            "useractivity",
                            uidHash
                        ));
                    } else {
                        result.add(new MirrorDestination(
                            "conversions", 
                            uidHash
                        ));
                    }
                }
                else if (action.equals("IMPRESSION") && fields.get("objType").equals("AD"))
                {
                    result.add(new MirrorDestination(
                        "adviews", 
                        uidHash
                    ));
                }
                else if (action.equals("CLICK") && fields.get("objType").equals("AD"))
                {
                    result.add(new MirrorDestination(
                        "adclicks", 
                        uidHash
                    ));
                }
                else if (action.equals("PAGE_VIEW"))
                {
                    result.add(new MirrorDestination(
                        "pageviews", 
                        uidHash
                    ));
                }
            } else if (eventType.equals("VDNAQuizUserAnswer")) { // quiz engine message
                result.add(new MirrorDestination(
                    "useractivity", 
                    uidHash
                ));
            } else {
                log.warn("Unknown event_type "  + eventType+ " " + json);
                return result;
            }

            //Metrics
            input.mark();
            output.mark(result.size());

            /*
             * If the event didn't end up in any primary topic, it's a problem.
             */
            if (result.size() == 0)
            {
                log.warn("No primary topic for event type `" + eventType + "`: " + json);
            }
        } catch (Exception e) {
            log.error("DesinationResolver encountered serious error while mapping the incoming message to output ", e);
        }

        // Absolute granularity metrics 
        try {
            //Double timestamp = Double.valueOf(fields.get("timestamp")) * 1000;
            /* TODO JIRA-35 Implement MBean
            if (timestamp > CentralMirror.latestObservedTimestamp)
            {
                CentralMirror.latestObservedTimestamp = timestamp;
            }
            if (timestamp < CentralMirror.earliestObservedTimestamp)
            {
                CentralMirror.earliestObservedTimestamp = timestamp;
            }
            */
        } catch (Exception e)
        {
            log.error("Could not extract timestamp from the json event", e);
        }

        return result;
    }

    private String parseMinimumJsonMessage(Message message, Map<String,String> fields) throws IOException
    {
        ByteBuffer buffer = message.payload();
        byte [] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String json = new String(bytes);

        //parse only the necessary fields (streaming jackson)
        JsonParser jp;
        try {
            jp = jsonFactory.createJsonParser(bytes);
        } catch (JsonParseException e) {
            log.warn("Message not in JSON format: " + new String(bytes));
            throw new IOException("Invalid message json, see logs for more details", e);
        }
        try {
            int filled = 0;
            while(jp.nextToken() != null && filled < fields.size())
            {
                if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
                    String fieldName = jp.getCurrentName();
                    jp.nextToken();
                    String value = jp.getText();
                    if (fields.containsKey(fieldName))
                    {
                        value = value.equals("0") ? null : value;
                        fields.put(fieldName, value);
                        ++filled;
                    }
                }
            }
            return json;
        } finally {
            jp.close();
        }

    }
}