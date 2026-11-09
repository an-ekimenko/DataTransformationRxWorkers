package fi.jackson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;

import fi.model.EmitentData_v3;
import fi.model.F12DataFromApi_v3;
import fi.model.F12ItemFromApi_v3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static by.beloil.shared.utils.NumericParser.parseSafeDouble;

/**
 * Custom Jackson deserializer responsible for parsing and translating raw JSON array structures 
 * from the external API into structured F12DataFromApi_v3 object models.
 * Calculates aggregated financial metrics (Return on Sales, Net Profit, Revenue, and Profit) on the fly.
 */
public class F12_DataDeserializer_v3 extends StdDeserializer<F12DataFromApi_v3> {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = LogManager.getLogger(F12_DataDeserializer_v3.class);

	/**
	 * Default no-arg constructor required by the Jackson framework for standard serialization hooks.
	 */
	public F12_DataDeserializer_v3() { 
		this(null); 
	}

	/**
	 * Parameterized constructor used to bind the target streaming mapping class context.
	 * 
	 * @param vc the target class type to deserialize
	 */
	public F12_DataDeserializer_v3(Class<?> vc) { 
		super(vc); 
	}
	
	/**
	 * Decodes the incoming JSON payload array, processes granular rows per issuer (Emitent), 
	 * computes target KPIs according to strict accounting row rules, and compiles them into a unified collection.
	 * 
	 * @param parser the active low-level JSON parser stream handler
	 * @param context the structural context of the current deserialization loop execution
	 * @return the fully populated and initialized F12DataFromApi_v3 root descriptor instance
	 * @throws IOException if a generic stream boundary network exception occurs
	 * @throws JacksonException if the incoming payload breaks mapped architectural schemas
	 */
	@Override
	public F12DataFromApi_v3 deserialize(JsonParser parser, DeserializationContext context) 
												throws IOException, JacksonException {
		logger.debug("Executing custom streaming deserialization and financial mapping for Form-12 payload...");
		ArrayNode nodes = parser.getCodec().readTree(parser);
    	
    	ArrayList<F12ItemFromApi_v3> f12_data = new ArrayList<>();
    	
        for (JsonNode node : nodes) {
        	String CodEmitent = node.get("CodEmitent").textValue();
        	
        	int Year = node.get("Year").intValue();
        	int Month = node.get("Month").intValue();
        	int Status = node.get("Status").intValue();
        	
        	JsonNode emitentRow = node.get("Rows");
        	if (emitentRow != null && emitentRow.isArray()) {
            	
        		EmitentData_v3 emitentData = new EmitentData_v3();
            	
        		Iterator<JsonNode> rowItems = emitentRow.elements(); 
        		String Code = null;
        		double Value = 0;
        		
        		double NetProfit = 0;
        		double Profit = 0;
            	double ReturnOnSales = 0;
            	double Revenue = 0;
        		
                while (rowItems.hasNext()) {
                	JsonNode item = rowItems.next();
                    Iterator<String> fieldNames = item.fieldNames();
                	
                    while (fieldNames.hasNext()) {
                    	String fieldName = fieldNames.next(); 
                    	switch (fieldName) {
                    		case "Code" -> Code = item.get(fieldName).asText();  
                    		case "Value" -> { 
                    				Value = parseSafeDouble(item.get(fieldName).asText());
                    				switch (Code.trim()) {
                            			case "001" -> { 
                            				if (Value > 0) {
                            					ReturnOnSales = 100 / Value;
                            					Revenue = Value;
                            				}	
                            			}
                            			case "019" -> { 
                            				ReturnOnSales = ReturnOnSales * Value;
                            				Profit = Value;
                            			}
                            			case "022", "023", "025", "026" -> NetProfit += Value;
                            			case "024", "027" -> NetProfit -= Value; 
                    				}
                    		}
                    	}
                    }
                }
                
                // Commit dynamically aggregated financial attributes into the issuer record map
                emitentData.putIfAbsent("019/001", ReturnOnSales);
                emitentData.putIfAbsent("028", NetProfit);
                emitentData.putIfAbsent("001", Revenue);
                emitentData.putIfAbsent("019", Profit);
                
                f12_data.add(new F12ItemFromApi_v3(CodEmitent, Year, Month, Status, emitentData)); 
        	}
        }
        
        logger.info("Financial json mapping finalized: successfully parsed and computed metrics for {} issuer entries.", f12_data.size());
        return new F12DataFromApi_v3(f12_data);
	}
	
	
}