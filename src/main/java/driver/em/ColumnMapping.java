package driver.em;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import java.util.Date;
import java.util.Map;


import com.datastax.driver.core.DataType;

public class ColumnMapping {

	protected String name;
	
	protected Field field;
	
	/**
	 * Currently only single map type is supported
	 * If we want to support more we would have
	 * a map DataType support - see TypeCodec
	 */
	protected boolean isMap = false;
	
	/**
	 * Mapping back to the CQL data type
	 */
	protected DataType type;
	
	
	protected Method getter;
	
	protected Method setter;
	
	public ColumnMapping(String name,Field field) {
		this.name = name;
		this.field = field;
		discoverBeanMethods();
		
		
	}
	public ByteBuffer getBuffer(Object value) {
		Class<?> cls = value.getClass();
		TypeCodec codec = TypeCodec.createFor(type.getName());
		
		ByteBuffer b = codec.serialize(value);
		
		return b;
	}

	protected void set(Object src,Object value) {
		if (setter !=null) {
			try {
				setter.invoke(src, value);
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else {
			try {
				field.set(src, value);
			} catch (IllegalAccessException| IllegalArgumentException e) {
				throw new IllegalAccessError("A configuration exception in getting field: " + field);
			}
		}
	}
	//throw a configuration exception?
	protected Object get(Object src) {
		Object ret = null;
		
		if (getter != null)
			try {
				ret = getter.invoke(src);
			} catch (InvocationTargetException | IllegalArgumentException | IllegalAccessException e) {
				throw new IllegalAccessError("A configuration exception in getting field: " + field);
			} 
		else
			try {
				ret = field.get(src);
			} catch (IllegalAccessException| IllegalArgumentException e) {
				throw new IllegalAccessError("A configuration exception in getting field: " + field);
			}
		return ret;
	}
	protected void discoverBeanMethods()  {
		//break this off?
		Class<?> ft = field.getType();
		if (String.class.equals(ft)) {
			type = DataType.text();
		}else if (int.class.equals(ft)) {
			type = DataType.cint();
		} else if (Integer.class.equals(ft)) {
			type = DataType.cint();
		}else if (int.class.equals(ft)) {
			type = DataType.cint();
		}else if (Long.class.equals(ft)) {
			type = DataType.bigint();
		}else if (long.class.equals(ft)) {
			type = DataType.bigint();
		}else if (Date.class.equals(ft)) {
			type = DataType.timestamp();
		}else if (ByteBuffer.class.equals(ft)) {
			type = DataType.blob();
		}
		//java.sql.timestamp is not directly supported
		//remove it for now
		/*else if (Timestamp.class.equals(ft)) {
			type = DataType.timestamp();
		}*/ else if (Map.class.isAssignableFrom(ft)) {
			type = DataType.map(DataType.text(), DataType.text());
		}
		
		//TODO and so and so
		try {
			String getMeth = "get" + field.getName().substring(0,1).toUpperCase() + field.getName().substring(1);
			String setMeth = "set" + field.getName().substring(0,1).toUpperCase() + field.getName().substring(1);
			getter = field.getDeclaringClass().getDeclaredMethod(getMeth);
			setter = field.getDeclaringClass().getDeclaredMethod(setMeth,field.getType());
		}catch (Exception e) {
			//TODO some useful stuff: warning of non-conformance to java bean conventions
		}
	}

}
