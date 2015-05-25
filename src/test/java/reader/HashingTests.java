package reader;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;


import junit.framework.Assert;

//import org.apache.cassandra.utils.MurmurHash;
import org.junit.Test;

import driver.em.Composite;

public class HashingTests {

	@Test
	public void test() {
		ByteBuffer partitionKey = ByteBuffer.wrap("24DF5769".getBytes());
		
		long hash = 0;//MurmurHash.hash3_x64_128(partitionKey, partitionKey.position(), partitionKey.remaining(), 0)[0];
		
		System.out.println("token: " + hash);
			
	}

	@Test
	public void testByteBufferEquality() {
		String one = "one";
		
		ByteBuffer bb = Composite.toByteBuffer(new Object[]{"one","two"});
		ByteBuffer bb2 = Composite.toByteBuffer(new Object[]{ByteBuffer.wrap(one.getBytes()),"two"});
		ByteBuffer bb3 = Composite.toByteBuffer(new Object[]{"two","one"});
		bb2.limit();bb2.array();
		
		//bb3.flip();
		Assert.assertTrue(bb.equals(bb2));
		Assert.assertFalse(bb.equals(bb3));
		
	}
}
