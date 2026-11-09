package fi.model;

import java.io.Serializable;
import java.util.Objects;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Data;

/**
 * Composite primary key for the F12t_Entity database persistent store.
 * Embedded inside the target MS SQL "DATA_KPI_SINGLE" staging table layout.
 * Maps custom physical database column names for calendar months and enterprise entity codes.
 */
@Data
@Embeddable 
public class F12t_PK implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Mapped database column representing the calculation calendar month interval.
	 * Explicitly maps to the physical database column named "calmonth".
	 */
	@Column(name = "calmonth") 
	private String YYYYMM;
	
	/**
	 * Mapped database column representing the corporate issuer or business entity identification code.
	 * Explicitly maps to the physical database column named "emt_code".
	 */
	@Column(name = "emt_code") 
	private String memberCode;

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		F12t_PK f12tPK = (F12t_PK) o;
		return Objects.equals(YYYYMM, f12tPK.YYYYMM) && 
			   Objects.equals(memberCode, f12tPK.memberCode);
	}

	@Override
	public int hashCode() {
		return Objects.hash(YYYYMM, memberCode);
	}
}