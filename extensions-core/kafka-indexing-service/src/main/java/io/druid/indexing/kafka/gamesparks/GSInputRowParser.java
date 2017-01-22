package io.druid.indexing.kafka.gamesparks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.logger.Logger;

import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.indexing.kafka.MongoUtils;

@JsonTypeName("gamesparks-parser")
public class GSInputRowParser extends StringInputRowParser {

	private static final Logger log = new Logger(GSInputRowParser.class);
	
	private static final ThreadLocal<DateFormat> sdf = new ThreadLocal<DateFormat>() {
		protected DateFormat initialValue() {
			TimeZone tz = TimeZone.getTimeZone("UTC");
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
			df.setTimeZone(tz);
			return df;
		};
	};

	static ObjectMapper om = new ObjectMapper();

	@JsonCreator
	public GSInputRowParser(@JsonProperty("parseSpec") ParseSpec parseSpec, @JsonProperty("encoding") String encoding) {
		super(parseSpec, encoding);
	}

	@Override
	public InputRow parse(String input) {

		try {
			JsonNode n = om.readTree(input);

			return buildRowFromJsonNode(n);

		} catch (JsonProcessingException e) {
			log.error(input);
			e.printStackTrace();
		} catch (IOException e) {
			log.error(input);
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public InputRow parse(ByteBuffer input) {
		try {
			JsonNode n = om.readTree(input.array());

			return buildRowFromJsonNode(n);

		} catch (Throwable t) {
			t.printStackTrace();
			throw new com.metamx.common.parsers.ParseException(t, "Unknow error");
		}

	}

	private static InputRow buildRowFromJsonNode(JsonNode n) {
		Date ts = MongoUtils.dateFromOidString(n.get("_id").get("$oid").asText());
		
		GSInputRow row = new GSInputRow(ts.getTime());

		row.addSharedDimension("timestamp", sdf.get().format(ts));
		row.addPlatformDimension("apiKey", n.get("_gameId").asText());
		
		JsonNode sessionNode = n.get("@sessionId");
		if (sessionNode != null && !sessionNode.isNull()) {
			row.addSharedMetric("sessionId", sessionNode.asText());
		}
		
		JsonNode playerNode = n.get("@playerId");
		if (playerNode != null && !playerNode.isNull()) {
			String playerId = playerNode.asText();
			if (playerId.length() > 10) {
				if (ts.getTime() - MongoUtils.dateFromOidString(playerId).getTime() > 3600000) {
					row.addSharedMetric("newPlayers", playerId);
				} else {
					row.addSharedMetric("agedPlayers", playerId);
				}
				row.addSharedMetric("players", playerId);
			}
		}

		JsonNode segmentsNode = n.get("@segments");
		if (segmentsNode != null && !segmentsNode.isNull()) {
			List<String> segmentsList = new ArrayList<String>();
			
			Iterator<Entry<String, JsonNode>> fields = segmentsNode.fields();
			
			while(fields.hasNext()){
				Entry<String, JsonNode> entry = fields.next();
				segmentsList.add(entry.getKey() + "-" + entry.getValue().asText());
			}
			
			if(!segmentsList.isEmpty()){
				row.addGameDimension("segments", segmentsList);
			}
		}

		JsonNode experimentSegmentsNode = n.get("@experimentSegments");
		if (experimentSegmentsNode != null && !experimentSegmentsNode.isNull()) {
			Iterator<Entry<String, JsonNode>> fields = experimentSegmentsNode.fields();
			while(fields.hasNext()){
				Entry<String, JsonNode> entry = fields.next();
				row.addGameDimension("experimentSegment_" + entry.getKey(), entry.getValue().asText());
			}
		}

		JsonNode segmentQueriesNode = n.get("@segmentQueries");

		if (segmentQueriesNode != null && segmentQueriesNode.isArray()) {
			List<String> segmentQueries = new ArrayList<String>();
			Iterator<JsonNode> fields = segmentQueriesNode.iterator();
			while(fields.hasNext()){
				JsonNode node = fields.next();
				segmentQueries.add(node.textValue());
			}
			row.addGameDimension("segmentQueries", segmentQueries);
		
		}

		JsonNode statementsNode = n.get("statements");
		if (statementsNode != null && statementsNode.isNumber()) {
			row.addSharedMetric("scriptStatements", statementsNode.asInt());
		}

		JsonNode scriptDurationNode = n.get("scriptDuration");
		if (scriptDurationNode != null && !scriptDurationNode.isNull()) {
			row.addSharedMetric("scriptDuration", scriptDurationNode.asInt());
		}

		JsonNode durationNode = n.get("@duration");
		if (durationNode != null && !durationNode.isNull()) {
			row.addSharedMetric("totalDuration", durationNode.asInt());
		}
		
		JsonNode messageNode = n.get("message");
		if (messageNode != null && !messageNode.isNull()) {
			row.addSharedDimension("type", "message");
			row.addSharedDimension("api", messageNode.get("@class").asText().substring(1));
		}

		JsonNode requestNode = n.get("request");
		if (requestNode != null && !requestNode.isNull()) {
			row.addSharedDimension("type", "request");
			row.addSharedDimension("api", requestNode.get("@class").asText().substring(1));
			row.addSharedMetric("responseSize", n.get("response").toString().length());
		}
		
		return row;
	}

	public GSInputRowParser withParseSpec(ParseSpec parseSpec) {
		return new GSInputRowParser(getParseSpec(), getEncoding());
	}

}
