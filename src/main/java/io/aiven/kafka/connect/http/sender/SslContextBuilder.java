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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

final class SslContextBuilder {

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

    static SSLContext createSslContext(final HttpSinkConfig config) 
            throws NoSuchAlgorithmException, KeyManagementException {
        final SSLContext sslContext = SSLContext.getInstance("TLS");
        if (config.sslTrustAllCertificates()) {
            sslContext.init(null, new TrustManager[] {DUMMY_TRUST_MANAGER}, new SecureRandom());
        } else {
            final TrustManagerFactory tmf = loadTruststore(config);
            sslContext.init(null, tmf != null ? tmf.getTrustManagers() : null, new SecureRandom());
        }
        return sslContext;
    }

    private static TrustManagerFactory loadTruststore(final HttpSinkConfig config) {
        if (config.sslTruststoreLocation() == null) {
            return null;
        }
        try {
            final KeyStore trustStore = KeyStore.getInstance("JKS");
            final String path = config.sslTruststoreLocation();
            
            final InputStream is = TruststoreLoader.findTruststoreInputStream(path);
            if (is == null) {
                throw new RuntimeException("Truststore file not found: " + path
                    + ". Tried classpath and file system locations.");
            }
            
            try (InputStream finalIs = is) {
                trustStore.load(finalIs, config.sslTruststorePassword() != null 
                    ? config.sslTruststorePassword().toCharArray() : null);
            }
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            return tmf;
        } catch (KeyStoreException
                | IOException
                | NoSuchAlgorithmException
                | CertificateException e) {
            throw new RuntimeException("Failed to load truststore: " + config.sslTruststoreLocation(), e);
        }
    }
}
