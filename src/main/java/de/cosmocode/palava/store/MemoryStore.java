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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;

/**
 * Memory based implementation of the {@link Store} interface.
 *
 * @author Willi Schoenborn
 */
final class MemoryStore implements Store {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryStore.class);
    
    private final ConcurrentMap<UUID, byte[]> map = new MapMaker().makeMap();

    @Override
    public String create(InputStream stream) throws IOException {
        Preconditions.checkNotNull(stream, "Stream");
        final UUID uuid = UUID.randomUUID();
        
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        IOUtils.copy(stream, output);
        final byte[] data = output.toByteArray();
        LOG.trace("Storing {} to {}", stream, uuid);
        map.put(uuid, data);
        
        return uuid.toString();
    }
    
    @Override
    public InputStream read(String identifier) throws IOException {
        Preconditions.checkNotNull(identifier, "Identifier");
        final UUID uuid = UUID.fromString(identifier);

        LOG.trace("Reading data from {}", uuid);
        final byte[] data = map.get(uuid);
        
        if (data == null) {
            throw new IOException(String.format("Unknown identifier %s", uuid));
        } else {
            return new ByteArrayInputStream(data);
        }
    }
    
    @Override
    public void delete(String identifier) throws IOException {
        Preconditions.checkNotNull(identifier, "Identifier");
        final UUID uuid = UUID.fromString(identifier);

        LOG.trace("Removing {} from store", uuid);
        if (map.remove(uuid) == null) {
            throw new IOException(String.format("Unknown identifier %s", uuid));
        }
    }
    
}
