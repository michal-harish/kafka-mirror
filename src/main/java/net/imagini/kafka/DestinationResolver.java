package net.imagini.kafka;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

    static private Logger log = LoggerFactory.getLogger(DestinationResolver.class);
    static private JsonFactory jsonFactory = new JsonFactory();
    final private List<MirrorDestination> emptyDestinationList = new ArrayList<MirrorDestination>();
    private MessageDigest sha256;
    //private MessageDigest md5;

    public DestinationResolver()
    {
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            log.error("Could not initialize resolver", e);
        }
    }
    /**
     * Entry point method which decides how to resolve the message depending
     * on the source topic.
     */
    public List<MirrorDestination> resolve(MessageAndMetadata<Message> metaMsg)
    {
        if (metaMsg.topic().equals("tracking_events"))
        {
            return resolveTrackingEvents(metaMsg);
        } else if (metaMsg.topic().equals("sim_tracking_events"))
        {
            return resolveTrackingEvents(metaMsg);
        } else {
            log.warn("Unknown topic " + metaMsg.topic());
            return emptyDestinationList;
        }
    }

    private List<MirrorDestination> resolveTrackingEvents(MessageAndMetadata<Message> metaMsg)
    {
        ArrayList<MirrorDestination> result = new ArrayList<MirrorDestination>();
        try {
            //get payload bytes
            ByteBuffer buffer = metaMsg.message().payload();
            byte [] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);

            //prepare fields we're interetsed in for parititoning purpose
            HashMap<String,String> fields = new HashMap<String,String>();
            fields.put("timestamp", null);
            fields.put("date", null);
            fields.put("action", null);
            fields.put("objType", null);
            fields.put("objId", null);
            fields.put("userUid", null);
            fields.put("vdna_widget_mc", null);

            //read only the necessary fields (streaming jackson)
            try {
                JsonParser jp = jsonFactory.createJsonParser(bytes);
                int filled = 0;
                while(jp.nextToken() != null && filled < fields.size())
                {
                    if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
                        String fieldName = jp.getCurrentName();
                        jp.nextToken();
                        String value = jp.getText();
                        if (fieldName.equals("event_type") && !value.equals("esVDNAAppUserActionEvent"))
                        {
                            throw new IOException("Invalid event_type = \""+value+"\"");
                        } else if (fields.containsKey(fieldName))
                        {
                            value = value.equals("0") ? null : value;
                            //log.debug("EVENT FIELD " + fieldName + " " + value);
                            fields.put(fieldName, value);
                            ++filled;
                        }
                    }
                }
                jp.close();
            } catch (JsonParseException e) {
                log.warn("Message not in JSON format: " + new String(bytes)) ;
                e.printStackTrace();
                return result;
            } catch (IOException e) {
                log.error("Couldn'r read JSON ", e);
                return result;
            }

            //deterime and validate derived values
            String action = fields.get("action");
            if (action == null)
            {
                log.warn("Invalid event with null action" + new String(bytes)) ;
                return result;
            }

            String uuid = fields.get("userUid");
            if (uuid == null)
            {
                uuid = fields.get("vdna_widget_mc");
                //TODO validate the if this comes from the deterministic generator or a true uuid
            }
            String uuidHashString;
            /*byte[] bytesOfMessage;
            try {
                bytesOfMessage = uuid.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e1) {
                log.error("UTF-8 problem", e1);
                return result;
            }
            byte[] thedigest = sha256.digest(bytesOfMessage);
            String uuidHashString = new String(thedigest);
            uuidHashString += uuid;
            */
            uuidHashString = uuid;
            Integer uuidHash = (uuid == null ? null : Math.abs(uuidHashString.hashCode()));
    
            /*
             * Publish into one of the topics for each event type - e.g. primary topic.
             * Every event has to end up at least in one of the following.
             */
            if (action.startsWith("INSERTED_INTO_"))
            {
                result.add(new MirrorDestination(
                    "datasync", 
                    uuidHash
                ));
            }
            else if (action.equals("CONVERSION") && fields.get("objId").equals("sync"))
            {
                result.add(new MirrorDestination(
                    "datasync", 
                    uuidHash
                ));
            }
            else if (action.equals("MINTED_USER_KEY"))
            {
                result.add(new MirrorDestination(
                    "datasync", 
                    uuidHash
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
                        uuidHash
                    ));
                } else {
                    result.add(new MirrorDestination(
                        "conversions", 
                        uuidHash
                    ));
                }
            }
            else if (action.equals("IMPRESSION") && fields.get("objType").equals("AD"))
            {
                result.add(new MirrorDestination(
                    "adviews", 
                    uuidHash
                ));
            }
            else if (action.equals("CLICK") && fields.get("objType").equals("AD"))
            {
                result.add(new MirrorDestination(
                    "adclicks", 
                    uuidHash
                ));
            }
            else if (action.equals("PAGE_VIEW"))
            {
                result.add(new MirrorDestination(
                    "pageviews", 
                    uuidHash
                ));
            }
    
            /*
             * If the event didn't end up in any primary topic, it's a problem.
             */
            if (result.size() == 0)
            {
                //&& action.equals("UPDATED_IDENTITY")// old cached event snippet - ignore
                log.warn("No primary topic for event type `" + action + "`: " + new String(bytes));
            }
    
            //Collect metrics
            /*
            try {
                Double timestamp = Double.valueOf(fields.get("timestamp")) * 1000;
                if (timestamp > CentralMirror.latestObservedTimestamp)
                {
                    CentralMirror.latestObservedTimestamp = timestamp;
                }
                if (timestamp < CentralMirror.earliestObservedTimestamp)
                {
                    CentralMirror.earliestObservedTimestamp = timestamp;
                }
            } catch (Exception e)
            {
                log.error("Could not extract timestamp from the json event", e);
            }
            */
        } catch (Exception e3) {
            log.error("DesinationResolver encountered serious error ", e3);
        }

        return result;
    }
}