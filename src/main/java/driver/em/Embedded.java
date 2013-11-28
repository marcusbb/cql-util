package driver.em;

import java.lang.reflect.Field;

public class Embedded extends ColumnMapping {

	
	
	protected ColumnMapping[] columns;
	
	public Embedded(Field field,ColumnMapping[] columns) {
		super(null,field);
		
		this.columns = columns;
		
	}

	public ColumnMapping get(String colName) {
		for (ColumnMapping mapping:columns) {
			if (mapping.name.equals(colName))
				return mapping;
		}
		return null;
	}
	
}
