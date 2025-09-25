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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ProxySelector;
import java.net.Socket;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

public final class HttpSenderFactory {

    public static HttpSender createHttpSender(final HttpSinkConfig config) {
        final var client = buildHttpClient(config);
        switch (config.authorizationType()) {
            case NONE:
                return new DefaultHttpSender(config, client);
            case STATIC:
                return new StaticAuthHttpSender(config, client);
            case OAUTH2:
                final OAuth2AccessTokenHttpSender oauth2AccessTokenHttpSender =
                    new OAuth2AccessTokenHttpSender(config, client);
                return new OAuth2HttpSender(config, client, oauth2AccessTokenHttpSender);
            default:
                throw new ConnectException("Can't create HTTP sender for auth type: " + config.authorizationType());
        }
    }

    private static final TrustManager DUMMY_TRUST_MANAGER = new X509ExtendedTrustManager() {
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType, final Socket socket)
            throws CertificateException {

        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType, final Socket socket)
            throws CertificateException {

        }

        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType, final SSLEngine engine)
            throws CertificateException {

        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType, final SSLEngine engine)
            throws CertificateException {

        }

        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new java.security.cert.X509Certificate[0];
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType)
            throws CertificateException {

        }

        @Override
        public void checkServerTrusted(final java.security.cert.X509Certificate[] chain, final String authType)
            throws CertificateException {
        }
    };

    static HttpClient buildHttpClient(final HttpSinkConfig config) {
        final var clientBuilder = HttpClient.newBuilder();
        if (config.hasProxy()) {
            clientBuilder.proxy(ProxySelector.of(config.proxy()));
        }
        if (config.sslTrustAllCertificates() || config.sslKeystoreLocation() != null) {
            try {
                final SSLContext sslContext = SSLContext.getInstance("TLS");
                if (config.sslTrustAllCertificates()) {
                    sslContext.init(null, new TrustManager[] {DUMMY_TRUST_MANAGER}, new SecureRandom());
                } else {
                    final KeyManagerFactory kmf = loadKeystore(config);
                    sslContext.init(kmf != null ? kmf.getKeyManagers() : null, null, new SecureRandom());
                }
                clientBuilder.sslContext(sslContext);
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                throw new RuntimeException(e);
            }
        }
        return clientBuilder.build();
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private static KeyManagerFactory loadKeystore(final HttpSinkConfig config) {
        if (config.sslKeystoreLocation() == null) {
            return null;
        }
        try {
            final KeyStore keyStore = KeyStore.getInstance("JKS");
            final String path = config.sslKeystoreLocation();
            System.out.println("DEBUG: Looking for keystore at path: " + path);
            System.out.println("DEBUG: Class classloader: " + HttpSenderFactory.class.getClassLoader());
            System.out.println("DEBUG: Context classloader: " + Thread.currentThread().getContextClassLoader());
            
            InputStream is = null;
            
            // Try 1: Class-based resource loading
            System.out.println("DEBUG: Trying class-based resource loading: " + path);
            is = HttpSenderFactory.class.getResourceAsStream(path);
            if (is != null) {
                System.out.println("DEBUG: Found via class-based resource loading");
            }
            
            // Try 2: Context classloader
            if (is == null) {
                System.out.println("DEBUG: Trying context classloader: " + path);
                is = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(path.startsWith("/") ? path.substring(1) : path);
                if (is != null) {
                    System.out.println("DEBUG: Found via context classloader");
                }
            }
            
            // Try 3: File system - same directory as JAR
            if (is == null) {
                try {
                    final URL jarLocation = HttpSenderFactory.class.getProtectionDomain().getCodeSource().getLocation();
                    System.out.println("DEBUG: JAR location: " + jarLocation);
                    final Path jarPath = Paths.get(jarLocation.toURI());
                    final Path parentPath = jarPath.getParent();
                    if (parentPath == null) {
                        System.out.println("DEBUG: JAR has no parent directory, skipping file system lookup");
                    } else {
                        final Path keystorePath = parentPath.resolve(path.startsWith("/") ? path.substring(1) : path);
                        System.out.println("DEBUG: Trying file system path: " + keystorePath);
                        final File keystoreFile = keystorePath.toFile();
                        if (keystoreFile.exists()) {
                            System.out.println("DEBUG: Found via file system");
                            is = new FileInputStream(keystoreFile);
                        }
                    }
                } catch (final URISyntaxException e) {
                    System.out.println("DEBUG: Failed to resolve JAR path: " + e.getMessage());
                }
            }
            
            if (is == null) {
                throw new RuntimeException("Keystore file not found: " + path
                    + ". Tried classpath and file system locations.");
            }
            
            try (InputStream finalIs = is) {
                keyStore.load(finalIs, config.sslKeystorePassword() != null 
                    ? config.sslKeystorePassword().toCharArray() : null);
            }
            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, config.sslKeystorePassword() != null 
                ? config.sslKeystorePassword().toCharArray() : null);
            return kmf;
        } catch (KeyStoreException
                | IOException
                | NoSuchAlgorithmException
                | CertificateException
                | UnrecoverableKeyException e) {
            throw new RuntimeException("Failed to load keystore: " + config.sslKeystoreLocation(), e);
        }
    }
}
