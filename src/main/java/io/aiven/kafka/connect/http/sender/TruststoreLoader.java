/*
 * Copyright 2023 Aiven Oy and http-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.http.sender;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

final class TruststoreLoader {

    static InputStream findTruststoreInputStream(final String path) {
        System.out.println("DEBUG: Looking for truststore at path: " + path);
        
        InputStream is = tryClasspathResource(path);
        if (is != null) {
            return is;
        }
        
        is = tryContextClassloader(path);
        if (is != null) {
            return is;
        }
        
        return tryFileSystem(path);
    }
    
    private static InputStream tryClasspathResource(final String path) {
        System.out.println("DEBUG: Trying class-based resource loading: " + path);
        final InputStream is = TruststoreLoader.class.getResourceAsStream(path);
        if (is != null) {
            System.out.println("DEBUG: Found via class-based resource loading");
        }
        return is;
    }
    
    private static InputStream tryContextClassloader(final String path) {
        System.out.println("DEBUG: Trying context classloader: " + path);
        final InputStream is = Thread.currentThread().getContextClassLoader()
            .getResourceAsStream(path.startsWith("/") ? path.substring(1) : path);
        if (is != null) {
            System.out.println("DEBUG: Found via context classloader");
        }
        return is;
    }
    
    private static InputStream tryFileSystem(final String path) {
        try {
            final URL jarLocation = TruststoreLoader.class.getProtectionDomain().getCodeSource().getLocation();
            System.out.println("DEBUG: JAR location: " + jarLocation);
            final Path jarPath = Paths.get(jarLocation.toURI());
            final Path parentPath = jarPath.getParent();
            if (parentPath == null) {
                System.out.println("DEBUG: JAR has no parent directory, skipping file system lookup");
                return null;
            }
            final Path truststorePath = parentPath.resolve(path.startsWith("/") ? path.substring(1) : path);
            System.out.println("DEBUG: Trying file system path: " + truststorePath);
            final File truststoreFile = truststorePath.toFile();
            if (truststoreFile.exists()) {
                System.out.println("DEBUG: Found via file system");
                return new FileInputStream(truststoreFile);
            }
        } catch (final URISyntaxException | IOException e) {
            System.out.println("DEBUG: Failed to resolve JAR path: " + e.getMessage());
        }
        return null;
    }
}
