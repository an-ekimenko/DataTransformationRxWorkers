package acn.jackson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import acn.model.ACN;
import acn.model.ACN_Data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static by.beloil.shared.utils.NumericParser.parseSafeDouble;

/**
 * Custom Jackson deserializer responsible for parsing and transforming raw JSON payloads 
 * received from the telemetry endpoints into corporate ACN_Data domain object models.
 */
public class ACN_DataDeserializer extends StdDeserializer<ACN_Data> {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = LogManager.getLogger(ACN_DataDeserializer.class);

	/**
	 * Default no-arg constructor required by the Jackson reflection mapping sub-system.
	 */
	public ACN_DataDeserializer() { 
		this(null); 
	}

	/**
	 * Parameterized constructor used to bind the target streaming mapping class context.
	 * 
	 * @param vc the target class type to deserialize
	 */
	public ACN_DataDeserializer(Class<?> vc) { 
		super(vc); 
	}

	/**
	 * Intercepts the incoming JSON token stream, extracts calendar metadata nodes, 
	 * maps array elements into granular DTO instances, and builds the root object layout.
	 * 
	 * @param jp the active low-level JSON parser stream handler
	 * @param ctxt the structural context of the current deserialization loop execution
	 * @return the fully populated and initialized ACN_Data model root descriptor instance
	 * @throws IOException if a generic stream boundary network exception occurs
	 * @throws JsonProcessingException if the incoming payload breaks mapped architectural schemas
	 */
	@Override
	public ACN_Data deserialize(JsonParser jp, DeserializationContext ctxt) 
			throws IOException, JsonProcessingException {
		
		logger.debug("Executing custom streaming deserialization sequence for incoming ACN network data object...");
		
		// Map the raw stream directly onto an immutable JSON structural node tree
		JsonNode rootNode = jp.getCodec().readTree(jp);
		ACN_Data acnData = new ACN_Data();

		// Extract the global reporting date string identifier token (e.g., YYYYMM layout context)
		JsonNode dateStringNode = rootNode.get("dateString");
		if (dateStringNode != null) {
			acnData.setYYYYMM(dateStringNode.asText());
			logger.debug("Successfully mapped current payload reporting timeline interval: {}", acnData.getYYYYMM());
		} else {
			logger.warn("Payload structure variance: 'dateString' root node mapping identifier is missing.");
		}

		// Extract and parse the underlying array holding active client metrics data
		JsonNode acliveClientsNode = rootNode.get("acliveClients");
		if (acliveClientsNode != null && acliveClientsNode.isArray()) {
			ArrayList<ACN> acnList = new ArrayList<>();
			Iterator<JsonNode> elements = acliveClientsNode.elements();

			while (elements.hasNext()) {
				JsonNode element = elements.next();
				
				// Validate presence of critical structural telemetry parameters before mapping
				if (element.has("memberCode")) {
					ACN record = new ACN();
					
					record.setMemberCode(element.get("memberCode").asInt());

					record.setActiveClients_Month(element.has("activeClients_Month") ? parseSafeDouble(element.get("activeClients_Month").asText()) : 0.0);
					record.setActiveClients_Year(element.has("activeClients_Year") ? parseSafeDouble(element.get("activeClients_Year").asText()) : 0.0);

					
					acnList.add(record);
				}
			}
			
			acnData.setAcnList(acnList);
			logger.info("Custom stream binding finalized: extracted {} active client analytics items from payload array.", acnList.size());
		} else {
			logger.warn("Payload verification criteria failed: 'acliveClients' structural JSON array node is empty or missing.");
		}

		return acnData;
	}
	

}
