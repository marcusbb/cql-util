package driver.em;

import java.nio.ByteBuffer;
///////////////////////////////////////////////////////
/*
INSERTED AS A CONVENIENCE UNTIL THE DRIVER 2.0 IS BROUGHT ONLINE
*/
///////////////////////////////////////////////////////
/**
 * A regular (non-prepared and non batched) CQL statement.
 * <p>
 * This class represents a query string along with query options (and optionally
 * binary values, see {@code getValues}). It can be extended but {@link SimpleStatement}
 * is provided as a simple implementation to build a {@code RegularStatement} directly
 * from its query string.
 */
@Deprecated
public abstract class RegularStatement extends Statement {

    /**
     * Returns the query string for this statement.
     *
     * @return a valid CQL query string.
     */
    public abstract String getQueryString();

    /**
     * The values to use for this statement.
     *
     * @return the values to use for this statement or {@code null} if there is
     * no such values.
     *
     * @see SimpleStatement#SimpleStatement(String, Object...)
     */
    public abstract ByteBuffer[] getValues();

    @Override
    public String toString() {
        return getQueryString();
    }
}
