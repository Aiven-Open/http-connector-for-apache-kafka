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

import java.io.IOException;
import java.io.InputStream;
import java.net.ProxySelector;
import java.net.Socket;
import java.net.http.HttpClient;
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

    private static KeyManagerFactory loadKeystore(final HttpSinkConfig config) {
        if (config.sslKeystoreLocation() == null) {
            return null;
        }
        try {
            final KeyStore keyStore = KeyStore.getInstance("JKS");
            try (InputStream is = HttpSenderFactory.class.getResourceAsStream(config.sslKeystoreLocation())) {
                keyStore.load(is, config.sslKeystorePassword() != null 
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
