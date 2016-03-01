/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.schema;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.db.AbstractVirtualColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public final class VirtualTableParams
{
    private static final Logger logger = LoggerFactory.getLogger(VirtualTableParams.class);

    public enum Option
    {
        CLASS;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    private final Class<? extends AbstractVirtualColumnFamilyStore> klass;
    private final ImmutableMap<String, String> options;

    private VirtualTableParams(Class<? extends AbstractVirtualColumnFamilyStore> klass, Map<String, String> options)
    {
        this.klass = klass;
        this.options = ImmutableMap.copyOf(options);
    }

    public static VirtualTableParams create(Class<? extends AbstractVirtualColumnFamilyStore> klass, Map<String, String> options)
    {
        Map<String, String> allOptions = new HashMap<>(options);

        return new VirtualTableParams(klass, allOptions);
    }

    public void validate()
    {
        try
        {
            Map<?, ?> unknownOptions = (Map) klass.getMethod("validateOptions", Map.class).invoke(null, options);
            if (!unknownOptions.isEmpty())
            {
                throw new ConfigurationException(format("Properties specified %s are not understood by %s",
                                                        unknownOptions.keySet(),
                                                        klass.getSimpleName()));
            }
        }
        catch (NoSuchMethodException e)
        {
            logger.warn("Virtual table {} does not have a static validateOptions method. Validation ignored",
                        klass.getName());
        }
        catch (InvocationTargetException e)
        {
            if (e.getTargetException() instanceof ConfigurationException)
                throw (ConfigurationException) e.getTargetException();

            Throwable cause = e.getCause() == null
                            ? e
                            : e.getCause();

            throw new ConfigurationException(format("%s.validateOptions() threw an error: %s %s",
                                                    klass.getName(),
                                                    cause.getClass().getName(),
                                                    cause.getMessage()),
                                             e);
        }
        catch (IllegalAccessException e)
        {
            throw new ConfigurationException("Cannot access method validateOptions in " + klass.getName(), e);
        }
    }

    public Class<? extends AbstractVirtualColumnFamilyStore> klass()
    {
        return klass;
    }

    /**
     * All strategy options - excluding 'class'.
     */
    public Map<String, String> options()
    {
        return options;
    }

    public static VirtualTableParams fromMap(Map<String, String> map)
    {
        Map<String, String> options = new HashMap<>(map);

        String className = options.remove(Option.CLASS.toString());
        if (className == null)
        {
            throw new ConfigurationException(format("Missing sub-option '%s' for the '%s' option",
                                                    Option.CLASS,
                                                    TableParams.Option.COMPACTION));
        }

        return create(classFromName(className), options);
    }

    private static Class<? extends AbstractVirtualColumnFamilyStore> classFromName(String name)
    {
        String className = name.contains(".")
                         ? name
                         : "org.apache.cassandra.db." + name;
        Class<AbstractVirtualColumnFamilyStore> strategyClass = FBUtilities.classForName(className, "virtual table");

        if (!AbstractVirtualColumnFamilyStore.class.isAssignableFrom(strategyClass))
        {
            throw new ConfigurationException(format("Compaction strategy class %s is not derived from AbstractVirtualColumnFamilyStore",
                                                    className));
        }

        return strategyClass;
    }

    public Map<String, String> asMap()
    {
        Map<String, String> map = new HashMap<>(options());
        map.put(Option.CLASS.toString(), klass.getName());
        return map;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("class", klass.getName())
                          .add("options", options)
                          .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof VirtualTableParams))
            return false;

        VirtualTableParams cp = (VirtualTableParams) o;

        return klass.equals(cp.klass) && options.equals(cp.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(klass, options);
    }
}
