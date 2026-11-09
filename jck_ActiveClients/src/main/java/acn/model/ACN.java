package acn.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object (DTO) representing Active Clients Number (ACN) telemetry records.
 * Used for streaming data transformations from JSON payloads to persistent database models.
 */
@Data
@NoArgsConstructor 
@AllArgsConstructor
public class ACN {
	
    /**
     * Unique identification code of the enterprise business entity or corporate member.
     */
	private int memberCode;

    /**
     * Aggregated metrics representing the total volume of active clients processed during the current reporting month.
     */
	private double activeClients_Month;

    /**
     * Aggregated metrics representing the cumulative total volume of active clients processed during the current calendar year.
     */
	private double activeClients_Year;

}