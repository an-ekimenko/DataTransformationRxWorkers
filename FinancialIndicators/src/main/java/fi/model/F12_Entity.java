package fi.model;

import java.io.Serializable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Persistent JPA Entity mapped to the "f12_f" database table inside the MS SQL staging layer.
 * Stores granular processed financial indicators and statement status codes.
 */
@Data
@NoArgsConstructor 
@AllArgsConstructor
@Entity 
@Table(name = "f12_f")
public class F12_Entity implements Serializable, Cloneable {
	
    private static final long serialVersionUID = 1L;

    /**
     * Composite primary key embedding the calendar period (YYYYMM), corporate member code, and specific accounting line code.
     */
    @EmbeddedId 
    private F12_PK id;

    /**
     * Operational validation status code of the underlying financial document record.
     */
    private int status;

    /**
     * Mapped database column holding the parsed or dynamically aggregated monetary value of the financial indicator.
     */
    private double value;
    
    /**
     * Creates and returns a shallow copy of this entity instance.
     * 
     * @return a clone of this transaction entity instance
     * @throws CloneNotSupportedException if the object layout cannot be copied natively
     */
    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}