package fi.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object (DTO) representing an individual financial record node for a corporate issuer.
 * Maps metadata and parsed row properties for the Form-12 accounting schema structure.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class F12ItemFromApi_v3 {

    /**
     * Unique identification code of the corporate issuer or business entity (Emitent).
     */
    private String CodeEmitent;

    /**
     * The calendar year of the financial reporting statement interval.
     */
    private int Year;

    /**
     * The calendar month of the financial reporting statement interval.
     */
    private int Month;

    /**
     * Operational processing status code of the financial document statement layout.
     */
    private int Status; 

    /**
     * Aggregated metrics container holding computed key performance indicator (KPI) attributes.
     */
    private EmitentData_v3 data;

}
