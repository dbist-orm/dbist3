package org.dbist.util;

import org.dbist.metadata.Column;

public interface ValueGenerator {
    public Object generate(Object data, Column column) throws Exception;
}
