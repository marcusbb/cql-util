package migration.poc;

import static driver.em.CharConst.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class RowToCqlMap extends RowToCql {

	protected JdbcColMapping nameCol;
	
	protected JdbcColMapping valCol;
	
	public RowToCqlMap(ResultSet rs,String cqlTable,JdbcColMapping[] mapping,JdbcColMapping nameCol,JdbcColMapping valCol ) {
		super(rs, cqlTable,mapping);
		this.nameCol = nameCol;
		this.valCol = valCol;
	}
	
	//TODO: again only support Map<String,String>
	@Override
	public String getCQL() throws SQLException {
		
		StringBuilder builder = new StringBuilder("UPDATE ").append(cqlTable);
				
		builder.append(" SET ");
				
		builder.append(nameCol.cqlName).append(openbsq).append(rs.getString(nameCol.jdbcName)).append(sqcloseb).append( eqParam);
						
		builder.append(getIdPredicate());
		
		
				
		return builder.toString();
	}
	
	@Override
	public Object[] values() throws SQLException {
		List<Object> values = new ArrayList<>();
		
		values.add(getValue(valCol));
		//iterate over pk columns
		for (JdbcColMapping mapping: colMapping) {
			if (mapping.isPK) {
				values.add(getValue(mapping));
			}
		}
		return values.toArray();
	}
	
}
