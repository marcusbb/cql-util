package driver.em;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.DataType;

public class CompositeTests {

	@Test
	public void testStringOnly() {
		test(new Object[]{"one","tow","three"});
	}
	@Test
	public void testStringLong() {
		test(new Object[]{"one","tow",new Long(3)});
	}

	@Test
	public void testPrimitives() {
		int i = Integer.MIN_VALUE;
		long l = Long.MAX_VALUE;
		
		test(new Object[]{i,l});
	}
	
	@Test
	public void testWithDates() {
		
	}
	public void test(Object []objs) {
		//Object []objs = {"one","two","three",new Long(0)};
		ByteBuffer bb = Composite.toByteBuffer(objs);
		
		assertNotNull(bb);
		List<DataType> dtList = new ArrayList<>();
		for (Object obj:objs) {
			dtList.add(TypeCodec.getDataTypeFor(obj));
		}
		List<Object> listObjs = Composite.fromByteBuffer(bb, dtList.toArray(new DataType[]{}));
		Assert.assertEquals(objs.length, listObjs.size());
		for (int i=0;i<listObjs.size();i++) {
			Assert.assertEquals(objs[i], listObjs.get(i));
		}
	}
}
