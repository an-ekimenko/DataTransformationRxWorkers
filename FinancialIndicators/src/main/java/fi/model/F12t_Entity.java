package fi.model;

import java.io.Serializable;
import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;

/**
 * Persistent JPA Entity mapped to the "DATA_KPI_SINGLE" staging database table inside the MS SQL environment.
 * Used as a transitional layer to store aggregated KPI indicators pulled from external historical sources.
 */
@Data
@Entity 
@Table(name = "DATA_KPI_SINGLE")
public class F12t_Entity implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Composite primary key embedding the calendar month (calmonth) and the issuer member code (emt_code).
     */
    @EmbeddedId 
    private F12t_PK id;

    /**
     * Mapped database column holding the transactional KPI baseline numerical value.
     */
    @Column(name = "value_01t")
    private double value_01t;
    
    @Override
    public String toString() {
        if (id == null) {
            return "F12t_Entity [Uninitialized Primary Key] -> [" + value_01t + "]";
        }
        return id.getMemberCode() + "-" + id.getYYYYMM() + " [" + value_01t + "]";
    }
}
