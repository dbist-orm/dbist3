/**
 * Copyright 2011-2012 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbist.exception;

/**
 * @author Steve M. Jung
 * @since 2011. 6. 2. (version 0.0.1)
 */
@SuppressWarnings("serial")
public class DbistException extends Exception {
    public DbistException() {
        super();
    }

    public DbistException(String message) {
        super(message);
    }

    public DbistException(Throwable cause) {
        super(cause);
    }

    public DbistException(String message, Throwable cause) {
        super(message, cause);
    }
}
