package de.cosmocode.palava.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;

/**
 * Memory based implementation of the {@link ByteStore} interface.
 *
 * @author Willi Schoenborn
 */
final class MemoryByteStore implements ByteStore {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryByteStore.class);
    
    private final ConcurrentMap<UUID, byte[]> map = new MapMaker().makeMap();
    
    @Override
    public String create(ByteBuffer buffer) throws IOException {
        Preconditions.checkNotNull(buffer, "Buffer");
        final UUID uuid = UUID.randomUUID();
        
        final byte[] data = buffer.array();
        LOG.trace("Storing {} to {}", data, uuid);
        map.put(uuid, data);
        
        return uuid.toString();
    }

    @Override
    public ByteBuffer read(String identifier) throws IOException {
        Preconditions.checkNotNull(identifier, "Identifier");
        final UUID uuid = UUID.fromString(identifier);

        LOG.trace("Reading data from {}", uuid);
        final byte[] data = map.get(uuid);
        
        if (data == null) {
            throw new IOException(String.format("Unknown identifier %s", uuid));
        } else {
            return ByteBuffer.wrap(data);
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
