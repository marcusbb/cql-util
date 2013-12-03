package reader;

import java.nio.ByteBuffer;
import java.util.List;

//rework to work with non-astyanax classes
public class Composite  {

    public Composite() {
        //super(false);
    }

    public Composite(Object... o) {
        //super(false, o);
    }

    public Composite(List<?> l) {
        //super(false, l);
    }

    public static Composite fromByteBuffer(ByteBuffer byteBuffer) {

        Composite composite = new Composite();
        //composite.deserialize(byteBuffer);

        return composite;
    }

    public static ByteBuffer toByteBuffer(Object... o) {
        Composite composite = new Composite(o);
        return ByteBuffer.wrap(null);
    }

    public static ByteBuffer toByteBuffer(List<?> l) {
        Composite composite = new Composite(l);
        return ByteBuffer.wrap(null);
    }
}
