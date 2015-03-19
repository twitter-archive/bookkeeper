package org.apache.bookkeeper.shims;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ShimLoader<T> {

    static final Logger LOG = LoggerFactory.getLogger(ShimLoader.class);

    private static final Map<Class<?>, Constructor<?>> constructorCache =
            new ConcurrentHashMap<Class<?>, Constructor<?>>();

    protected final String componentName;
    protected final Class<T> shimInterface;

    protected final Version version;

    public ShimLoader(String componentName, Class<T> shimInterface) {
        this.componentName = componentName;
        this.shimInterface = shimInterface;
        this.version = new Version(componentName);
    }

    /**
     * Load given shim interface.
     *
     * @return shim instance.
     */
    public T load() {
        String shimVersion = version.getVersion();
        String shimName = shimInterface.getName() + shimVersion.replaceAll("\\.", "");
        try {
            LOG.info("Load shim {}, version {}", shimName, shimVersion);
            Class<?> shimClass = Class.forName(shimName);
            if (!shimInterface.isAssignableFrom(shimClass)) {
                throw new RuntimeException("Class " + shimClass + " is ");
            }
            return newInstance(shimClass.asSubclass(shimInterface));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load shim class " + shimName + " : ", e);
        }
    }

    /**
     * New instance for given class <i>theCls</i>.
     *
     * @param theCls
     *          class to instantiate
     * @return instance for given class <i>theCls</i>
     */
    private static <T> T newInstance(Class<T> theCls) {
        T result;
        try {
            Constructor<T> meth = (Constructor<T>) constructorCache.get(theCls);
            if (null == meth) {
                meth = theCls.getDeclaredConstructor();
                meth.setAccessible(true);
                constructorCache.put(theCls, meth);
            }
            result = meth.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

}
