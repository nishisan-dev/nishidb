package dev.nishisan.nishidb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Version produced by Maven, shared by Driver and DatabaseMetaData. */
final class DriverVersion {
    static final String VERSION = load();
    static final int MAJOR = Integer.parseInt(VERSION.split("\\.")[0]);
    static final int MINOR = Integer.parseInt(VERSION.split("\\.")[1]);

    private DriverVersion() {}

    private static String load() {
        try (InputStream stream = DriverVersion.class.getResourceAsStream("version.properties")) {
            if (stream == null) {
                throw new IllegalStateException("Missing JDBC version resource; build with Maven");
            }
            Properties properties = new Properties();
            properties.load(stream);
            String version = properties.getProperty("version", "");
            if (!version.matches("(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-SNAPSHOT)?")) {
                throw new IllegalStateException("Invalid JDBC build version: " + version);
            }
            return version;
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
