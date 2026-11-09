package fi.model;

import java.util.ArrayList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Root Data Transfer Object (DTO) wrapping the JSON payload collection received from the Financial Indicators API.
 * Contains the mapped collection of individual form-12 issuer item nodes.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class F12DataFromApi_v3 {

    /**
     * Internal array list holding the structured granular financial records parsed per corporate issuer.
     */
    private ArrayList<F12ItemFromApi_v3> f12_list;
    
}
