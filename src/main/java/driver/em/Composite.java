package driver.em;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 *  
 * Composite can be a mixed typed list of objects, unlike TypeCodec.list
 */
public class Composite  {

	List<Object> objList = new ArrayList<>();
	List<DataType> dataTypes = new ArrayList<>();
	
   
    //when you want to serialize
    public Composite(Object... o) {
    	objList = new ArrayList<>();
       for (Object obj:o) {
    	   objList.add((Object)obj);
    	   dataTypes.add(TypeCodec.getDataTypeFor(obj));
       }
    }

    public Composite(List<Object> l) {
        objList = l;
    }
    //when you want to deserialize
    public Composite (DataType ...dts ) {
    	for (DataType t:dts) {
    		dataTypes.add(t);
    	}
    }

    private static ByteBuffer pack(List<ByteBuffer> buffers, int elements, int size) {
        ByteBuffer result = ByteBuffer.allocate(2 + size);
        result.putShort((short)elements);
        for (ByteBuffer bb : buffers) {
            result.putShort((short)bb.remaining());
            result.put(bb.duplicate());
        }
        return (ByteBuffer)result.flip();
    }

  
    public  ByteBuffer serialize() {
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(objList.size());
        int size = 0;
        for (int i=0;i<objList.size();i++) {
        	DataType dtype = dataTypes.get(i);
            ByteBuffer bb = dtype.serialize(objList.get(i));
            bbs.add(bb);
            size += 2 + bb.remaining();
        }
        return pack(bbs, objList.size(), size);
    }

    private static int getUnsignedShort(ByteBuffer bb) {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }
    public List<Object> deserialize(ByteBuffer bytes) {
        try {
            ByteBuffer input = bytes.duplicate();
            int n = getUnsignedShort(input);
            List<Object> l = new ArrayList<Object>(n);
            for (int i = 0; i < n; i++) {
                int s = getUnsignedShort(input);
                byte[] data = new byte[s];
                input.get(data);
                ByteBuffer databb = ByteBuffer.wrap(data);
                l.add(dataTypes.get(i).deserialize(databb));
            }
            return l;
        } catch (BufferUnderflowException e) {
            throw new InvalidTypeException("Not enough bytes to deserialize list");
        }
    }
   
    public static List<Object> fromByteBuffer(ByteBuffer byteBuffer,DataType ...dts) {

        Composite composite = new Composite(dts);
        return composite.deserialize(byteBuffer);
        
        
    }

    public static ByteBuffer toByteBuffer(Object... o) {
        Composite composite = new Composite(o);

        return composite.serialize();
    }

    public static ByteBuffer toByteBuffer(List<Object> l) {
        Composite composite = new Composite(l);

        return composite.serialize();
    }
}
