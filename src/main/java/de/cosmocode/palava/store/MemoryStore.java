/**
 * palava - a java-php-bridge
 * Copyright (C) 2007-2010  CosmoCode GmbH
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package de.cosmocode.palava.store;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

/**
 * Memory based implementation of the {@link Store} interface.
 * 
 * @author Willi Schoenborn
 */
final class MemoryStore extends AbstractByteStore implements ByteStore {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryStore.class);
    
    private final ConcurrentMap<String, byte[]> map = new MapMaker().makeMap();

    private IdGenerator generator = new UUIDGenerator();
    
    @Inject(optional = true)
    void setGenerator(IdGenerator generator) {
        this.generator = Preconditions.checkNotNull(generator, "Generator");
    }
    
    @Override
    public String create(InputStream stream) throws IOException {
        Preconditions.checkNotNull(stream, "Stream");
        final String uuid = generator.generate();
        create(stream, uuid);
        return uuid;
    }
    
    @Override
    public void create(InputStream stream, String identifier) throws IOException {
        Preconditions.checkNotNull(stream, "Stream");
        Preconditions.checkState(map.get(identifier) == null, "Byte array for %s already present", identifier);
        
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        IOUtils.copy(stream, output);
        final byte[] data = output.toByteArray();
        LOG.trace("Storing {} to {}", stream, identifier);
        map.put(identifier, data);
    }

    @Override
    public ByteBuffer view(String identifier) throws IOException {
        Preconditions.checkNotNull(identifier, "Identifier");

        LOG.trace("Reading data from {}", identifier);
        final byte[] data = map.get(identifier);
        
        if (data == null) {
            throw new IOException(String.format("Unknown identifier %s", identifier));
        } else {
            return ByteBuffer.wrap(data);
        }
    }
    
    @Override
    public Set<String> list() throws IOException {
        return Sets.newHashSet(Collections2.transform(map.keySet(), Functions.toStringFunction()));
    }
    
    @Override
    public void delete(String identifier) throws IOException {
        Preconditions.checkNotNull(identifier, "Identifier");

        LOG.trace("Removing {} from store", identifier);
        if (map.remove(identifier) == null) {
            throw new IOException(String.format("Unknown identifier %s", identifier));
        }
    }
    
}
