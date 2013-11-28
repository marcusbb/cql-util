package driver.em;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * 
 * If this is a composite PK, then we need to route 
 * via it's first property.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface RoutingKey {

}
