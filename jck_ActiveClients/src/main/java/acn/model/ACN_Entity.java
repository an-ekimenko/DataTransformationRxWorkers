package acn.model;

import java.io.Serializable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Persistent JPA Entity mapped to the "ActiveClientsNumber" database table.
 * Contains aggregated monthly and annual statistics for corporate members.
 */
@Data
@NoArgsConstructor 
@AllArgsConstructor
@Entity 
@Table(name = "ActiveClientsNumber")
public class ACN_Entity implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Composite primary key embedding the calendar period (YYYYMM) and the member identification code.
     */
    @EmbeddedId 
    private ACN_PK id;

    /**
     * Mapped database column representing the total volume of active clients processed during the reporting month.
     */
    private double activeClients_Month;

    /**
     * Mapped database column representing the cumulative total volume of active clients processed during the calendar year.
     */
    private double activeClients_Year;

}
