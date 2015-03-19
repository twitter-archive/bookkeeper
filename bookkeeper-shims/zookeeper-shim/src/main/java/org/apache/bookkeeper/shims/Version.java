package org.apache.bookkeeper.shims;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utils to load version info from shim package.
 */
public class Version {

    static final Logger LOG = LoggerFactory.getLogger(Version.class);

    private final Properties properties;

    public Version(String component) {
        properties = new Properties();
        String versionFile = component + "-version.properties";
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(versionFile);
            if (null == is) {
                throw new IOException("Resource " + versionFile + " not found");
            }
            properties.load(is);
        } catch (IOException ex) {
            LOG.warn("Could not read {} : ", versionFile, ex);
        }
    }

    public String getVersion() {
        return properties.getProperty("version", "Unknown");
    }
}
