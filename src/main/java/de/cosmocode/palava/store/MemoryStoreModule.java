package de.cosmocode.palava.store;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;

/**
 * Binds the {@link Store} interface to {@link MemoryStore}.
 *
 * @author Willi Schoenborn
 */
public final class MemoryStoreModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(Store.class).to(MemoryStore.class).in(Singleton.class);
    }

}
