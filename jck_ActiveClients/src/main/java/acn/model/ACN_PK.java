package acn.model;

import java.io.Serializable;
import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Composite primary key for the ActiveClientsNumber entity.
 * Implements Serializable as required by JPA specifications for embeddable primary keys.
 */
@Data
@NoArgsConstructor 
@AllArgsConstructor
@Embeddable 
public class ACN_PK implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Reporting calendar period encoded as an integer in YYYYMM format (e.g., 202611).
     */
	@Column(name = "YYYYMM") 
	private int YYYYMM;
	
    /**
     * Unique identification code of the enterprise business entity or corporate member.
     */
	@Column(name = "memberCode") 
	private int memberCode;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ACN_PK that = (ACN_PK) o;
        return YYYYMM == that.YYYYMM &&
        		memberCode == that.memberCode;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(YYYYMM, memberCode);
    }

}