package acn.model;

import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Root Data Transfer Object (DTO) wrapping the JSON payload received from the API.
 * Maps the reporting calendar period and the corresponding collection of active client statistics.
 */
@Data
public class ACN_Data {
	
    /**
     * The reporting calendar period formatted as a string (typically YYYYMM).
     * Mapped from the "dateString" JSON property in the incoming network payload.
     */
	@JsonProperty("dateString") 
	private String YYYYMM;

    /**
     * The internal collection containing detailed telemetry records of active clients.
     * Mapped from the "acliveClients" JSON array in the incoming network payload.
     */
	@JsonProperty("acliveClients") 
	private ArrayList<ACN> acnList;

}