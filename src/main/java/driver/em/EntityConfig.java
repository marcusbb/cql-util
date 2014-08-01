package driver.em;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Id;
import javax.persistence.Table;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import static driver.em.CharConst.*;
/**
 * A data structure for holding entity configuration related to serializing to and from the 
 * CQL driver
 * 
 * A mechanism for simple bean discovery and annotation parsing.
 * 
 * All operations that generate a {@link SimpleStatement}, could be pulled out
 * and moved to the {@link AbstractEntityManager}.
 *  
 * 
 * Until, I have a reason they will stay in here.
 * 
 * 
 * @param <T>
 */
public class EntityConfig<T> {

	protected boolean jpaEntity;
	
	protected String tableName;
	
	//private Field idField;
	protected ColumnMapping idMapping;
	
	protected Embedded embedded;
	
	//private Class<?> embeddedId;
	
	protected Class<T> entityClass;
	
	protected static ColumnMapping[] emptyColArr = new ColumnMapping[0];
	
	/**
	 * The mapping of column names to fields
	 */
	protected Map<String,ColumnMapping> colsToFields = new HashMap<String, ColumnMapping>();
	
	
	public EntityConfig(Class<T> entityClass) {
		this.entityClass = entityClass;
	}
	
	/**
	 * No checking on the annotations - client could get unpredictable behaviour if 
	 * annotations aren't configured properly.
	 * 
	 * @return
	 */
	public void discover() {
		
		jpaEntity = entityClass.isAnnotationPresent(javax.persistence.Entity.class);
		Table table = (Table)entityClass.getAnnotation(Table.class);
		if (! jpaEntity || table == null)
			throw new UnsupportedOperationException(); //throw a more appropriate exception
		
		tableName = table.name();
				
		
		//Absolutely no validation annotation parsing!!
		//this will ALWAYS parse annotations in this order: ID, EmbeddedId, and Column
		
		Field []fields = entityClass.getDeclaredFields();
		for (Field f:fields) {
			
			//first Id annotation
			Annotation fan = f.getAnnotation(Id.class);
			if (fan != null) {
				Column col = (Column)f.getAnnotation(Column.class);
				idMapping = new ColumnMapping(col.name(),f);
				continue;
			}
			
			//Embedded ID
			fan = f.getAnnotation(EmbeddedId.class);
			if (fan != null) {
				
				ArrayList<ColumnMapping> emList = new ArrayList<>();
				
				for (Field inF:f.getType().getFields()) {
					if (inF.getAnnotation(Column.class) != null) {
						Column innerId = (Column)inF.getAnnotation(Column.class);
						//put column name in lower case as driver does
						emList.add(new ColumnMapping(innerId.name().toLowerCase(), inF));
					}
				}
				//column name
				embedded = new Embedded(f, emList.toArray(emptyColArr) );
								
				continue;
			}
			//finally get the column
			//there's special handling for map type - which is a definite area for improvement
			
			fan = f.getAnnotation(Column.class);
			if (fan != null){
				Column col = (Column)fan;
				ColumnMapping mapping = new ColumnMapping(col.name(), f);
				//special consideration for map
				if (Map.class.isAssignableFrom(f.getType())) {
					mapping.isMap = true;
				}
				//put in lower case as the driver
				colsToFields.put(col.name().toLowerCase(),mapping);
				
				continue;
			}
			
				
		}
		
	}

	public String getIdPredicate() {
		StringBuilder builder = new StringBuilder();
		builder.append(" where ");
		if (embedded == null) {
			builder.append(idMapping.name).append(eqParam);
		}else {
			int i = 0;
			for (ColumnMapping mapping:embedded.columns) {
				builder.append(mapping.name ).append( eqParam );
				if (i < embedded.columns.length-1)
					builder.append(" and ");
				i++;
			}
		}
		return builder.toString();
	}
	
	
	/**
	 * Might move this out to a utility class
	 * This has been moved to {@link AbstractEntityManager#getUpdateStatement(Object)} 
	 * @param obj
	 * @return
	 */
	@Deprecated
	public SimpleStatement getUpdateStatement(T obj )  {
		
		
		StringBuilder builder = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
		ArrayList<Object> valueList = new ArrayList<>();
		int i = 0;
		//go through the columns
		for (String col:colsToFields.keySet()) {
			
			ColumnMapping mapping = colsToFields.get(col);
			Object valueObj = mapping.get(obj);
			
			
			if (!mapping.isMap) {
				builder.append(getColUpdate(col) );
				valueList.add(valueObj);
				
			} else {
				Map<String,String> map = (Map<String,String>)valueObj;
				if (map != null) {
					int im = 0;
					for (String key:map.keySet()) {
						builder.append(col).append(openbsq).append(key).append(sqcloseb).append( eqParam);
						if (im++ < map.size() -1)
							builder.append(comma).append(space);
						
						valueList.add(map.get(key));
						
					}
				}
				
			}
			if (i++ < (colsToFields.size() -1)) 
				builder.append(comma).append(space);
			
			
		}
	
		builder.append(getIdPredicate());
		if (embedded != null) {
			try {
				//this needs some improvement to the embedded class
				Object idObj = embedded.field.get(obj);
				for (ColumnMapping membed:embedded.columns) {
					valueList.add(membed.get(idObj) );
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		SimpleStatement ss = new SimpleStatement(builder.toString(),valueList.toArray());
		//System.out.println("update ss : " + ss.getQueryString());
		return ss;
	}
	
	/**
	 * Guess we could parameterize the "key" class as well.
	 * @param idObj
	 * @return
	 */
	public SimpleStatement getAllQuery(Object idObj) {
		StringBuilder builder = new StringBuilder("select * from ").append(tableName);
		ArrayList<Object> valueList = new ArrayList<>();
		
	
		builder.append(getIdPredicate());
		if (embedded == null) {
			valueList.add(idObj);
		} else {
			for (ColumnMapping mapping:embedded.columns) {
				valueList.add(mapping.get(idObj));
			}
		}
		SimpleStatement ss = new SimpleStatement(builder.toString(),valueList.toArray());
		//System.out.println("query ss : " + ss.getQueryString());
		return ss;
	}
	
	public SimpleStatement getDelStatement(Object idObj) {
		StringBuilder builder = new StringBuilder("delete from ").append(tableName);
		ArrayList<Object> valueList = new ArrayList<>();
		
	
		builder.append(getIdPredicate());
		if (embedded == null) {
			valueList.add(idObj);
		} else {
			for (ColumnMapping mapping:embedded.columns) {
				valueList.add(mapping.get(idObj));
			}
		}
		SimpleStatement ss = new SimpleStatement(builder.toString(),valueList.toArray());
		
		return ss;
	}
	/**
	 * Might move this out to a utility class.
	 * 
	 * @param row
	 * @return
	 * @throws Exception
	 */
	//Each row corresponds to an entity
	//Exceptions are throw
	//could refactor this such as all the new instance creation
	
	/**
	 * Populates and iterates through a {@link Row} 
	 * using {@link ColumnDefinitions} provided by the driver.
	 * 
	 * @param row
	 * @return
	 */
	public T get(Row row)  {
		
		//Entity and its associated id object
		T entity = null;
		Object idObj = null;
		
		try {
			entity = entityClass.newInstance();
			idObj = null;
			ColumnMapping idcolmap = null;
			//Note that this early invocation means that
			//you can't populate on #setId in your java bean method
			if (embedded != null) {
				 idObj = embedded.field.getType().newInstance();
				 embedded.set(entity, idObj);
			}
			else {
				idObj = idMapping.field.getType().newInstance();
				idMapping.set(entity, idObj);
			}
			
		}catch (InstantiationException | IllegalAccessException e) {
			throw new IllegalAccessError("A configuration exception has occurred in creating entity: " + entityClass);
		}
		ColumnDefinitions metaData = row.getColumnDefinitions();
		List<Definition> defList = metaData.asList();
		
		for (Definition def:defList) {
			ColumnMapping mapping = colsToFields.get(def.getName());
			
						
			if (mapping == null) {
				
				//it could be an id column
				if (idMapping !=null && def.getName().equals(idMapping.name) ) {
					idMapping.set(entity, getValue(row, idMapping, def));
					continue;
				}
				
				//else: need to find it, possible point of refactor
				ColumnMapping nembed = embedded.get(def.getName());
				if (nembed != null)
					nembed.set(idObj, getValue(row, nembed, def));
				
				continue;
				
			}


			Object value = getValue(row, mapping, def);
			//set it
			if (value != null)
				mapping.set(entity, value);
		}
		return entity;
	}
	private Object getValue(Row row,ColumnMapping mapping,Definition def) {
		Object value = null;
		
		if (DataType.text().equals(mapping.type)) {
			value = row.getString(def.getName());
			
		}else if (DataType.blob().equals(mapping.type)) {
			value = row.getBytes(def.getName());
			
		} else if (DataType.map(DataType.text(),DataType.text()).equals(mapping.type)) {
			value = row.getMap(def.getName(), String.class, String.class);
			
		} else if (DataType.cint().equals(mapping.type)) {
			value = row.getInt(def.getName());
		} else if (DataType.bigint().equals(mapping.type)) {
			value = row.getLong(def.getName());
		} else if (DataType.timestamp().equals(mapping.type)) {
			value = row.getDate(def.getName());
		} else if (DataType.blob().equals(mapping.type)) {
			value = row.getBytes(def.getName());
		} else if (DataType.cdouble().equals(mapping.type)) {
			value = row.getBytes(def.getName());
		} else if (DataType.cfloat().equals(mapping.type) ) {
			value = row.getFloat(def.getName());
		} else if (DataType.inet().equals(mapping.type)) {
			value = row.getInet(def.getName());
		} else if (DataType.cboolean().equals(mapping.type)) {
			value = row.getBool(def.getName());
		} else if (DataType.uuid().equals(mapping.type)) {
			value = row.getUUID(def.getName());
		}
		//what if value is null? primitives won't like this
		return value;
	}
	//
	public String getColUpdate(String colName) {
		StringBuilder builder = new StringBuilder();
		ColumnMapping mapping = colsToFields.get(colName);
		
		//simple mapping
		if (!mapping.isMap) {
			builder.append(colName).append( " = ? ");
		}
		
		return builder.toString();
	}
}
