package driver.em;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * 
 * Make sure I get the "?" substitution right - hopefully that's it.
 *
 */
public class SimpleStatementTests {

	@Test
	public void testQueryBuilder() {
		SimpleStatement ss = new SimpleStatement("update table1 set name = ?, intval = ?, othermap['la'] = ?",new Object[]{"name",new Integer(0),"value"});
	
		String builtQuery = ss.buildQueryString();
		System.out.println(builtQuery);
		
	}
	
	@Test
	public void testIncompleteValues() {
		
		SimpleStatement ss = new SimpleStatement("update table1 set name = ?, intval = ?, othermap['la'] = ?",new Object[]{"name",new Integer(0)});
		
		String builtQuery = ss.buildQueryString();
		System.out.println(builtQuery);
		
	}

}
