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
package org.dbist.processor.impl;

import java.io.StringWriter;
import java.util.Map;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.dbist.processor.Preprocessor;

/**
 * @author Steve M. Jung
 * @since 2012. 2. 5. (version 0.0.1)
 */
public class VelocityPreprocessor implements Preprocessor {
    private VelocityEngine engine;

    public String process(String value, Map<String, ?> contextMap) throws Exception {
        if (engine == null) {
            synchronized (this) {
                if (engine == null) {
                    engine = new VelocityEngine();
                    engine.init();
                }
            }
        }

        StringWriter writer = new StringWriter();
        VelocityContext context = new VelocityContext(contextMap);
        engine.evaluate(context, writer, value, value);
        return writer.toString();
    }

}
