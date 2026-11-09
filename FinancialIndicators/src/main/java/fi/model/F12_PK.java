package fi.model;

import java.io.Serializable;
import java.util.Objects;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Composite primary key for the F12_Entity database persistent store.
 * Embedded inside the target MS SQL schema staging tables layout.
 * Implements Serializable as strictly required by Jakarta Persistence specifications.
 */
@Data 
@NoArgsConstructor 
@AllArgsConstructor
@Embeddable 
public class F12_PK implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Mapped database column representing the calculation calendar period formatted as YYYYMM (e.g., "202611").
     */
    @Column(name = "YYYYMM")
    private String YYYYMM;
	
    /**
     * Mapped database column representing the unique corporate business member or enterprise identification code.
     */
    @Column(name = "memberCode")
    private int memberCode;
	
    /**
     * Mapped database column representing the specific accounting row line code identifier (e.g., "028", "019/001").
     */
    @Column(name = "Code")
    private String Code;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        F12_PK f12PK = (F12_PK) o;
        return memberCode == f12PK.memberCode && 
               Objects.equals(YYYYMM, f12PK.YYYYMM) && 
               Objects.equals(Code, f12PK.Code);
    }

    @Override
    public int hashCode() {
        return Objects.hash(YYYYMM, memberCode, Code);
    }
}