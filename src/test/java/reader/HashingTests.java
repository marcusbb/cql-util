package reader;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.MurmurHash;
import org.junit.Test;

public class HashingTests {

	@Test
	public void test() {
		ByteBuffer partitionKey = ByteBuffer.wrap("24DF5769".getBytes());
		
		long hash = MurmurHash.hash3_x64_128(partitionKey, partitionKey.position(), partitionKey.remaining(), 0)[0];
		
		System.out.println("token: " + hash);
			
	}

}
