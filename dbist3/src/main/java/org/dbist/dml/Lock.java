/**
 * Copyright 2011-2012 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbist.dml;

/**
 * @author Steve M. Jung
 * @since 2012. 4. 29. (version 1.0.10)
 */
public class Lock {
    private Integer timeout;
    private Boolean withNoLock;

    public Lock() {
        super();
    }

    public Lock(int timeout) {
        super();
        this.timeout = timeout;
    }


    public Lock(boolean withLock) {
        super();
        this.withNoLock = !withLock;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Boolean getWithNoLock() {
        return withNoLock;
    }

    public void setWithNoLock(Boolean withNoLock) {
        this.withNoLock = withNoLock;
    }
}
