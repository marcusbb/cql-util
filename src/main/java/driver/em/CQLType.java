package driver.em;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.datastax.driver.core.DataType;

/**
 * 
 * CQL specific data types.
 * It may be inferred from the primitive of the field of each annotated entity
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface CQLType {

	//set blob as default
	DataType.Name dataType = DataType.Name.BLOB;
	
}
