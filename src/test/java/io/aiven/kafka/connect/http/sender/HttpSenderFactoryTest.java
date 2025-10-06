/*
 * Copyright 2024 Aiven Oy and http-connector-for-apache-kafka project contributors
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

import javax.security.auth.x500.X500Principal;

import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Map;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HttpSenderFactoryTest {

    private static final String TRUSTSTORE_FILENAME = "test-truststore.jks";
    private static final String TRUSTSTORE_PASSWORD = "password";

    // Path to the test truststore file created for SSL tests
    private Path truststorePath;

    @Test
    void testDisabledHostnameVerification() throws IOException, InterruptedException {
        final var config = new HttpSinkConfig(Map.of(
            "http.url", "https://wrong.host.badssl.com/",
            "http.authorization.type", "none",
            "http.ssl.trust.all.certs", "true"
        ));
        final var httpSender = HttpSenderFactory.buildHttpClient(config);
        final HttpRequest request = HttpRequest.newBuilder().GET()
            .uri(config.httpUri())
            .build();
        final var response = httpSender.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
    }

    @BeforeEach
    void setUp() throws Exception {
        // Create truststore in the same directory as compiled test classes
        final var jarLocation = HttpSenderFactoryTest.class.getProtectionDomain().getCodeSource().getLocation();
        final Path jarPath = Paths.get(jarLocation.toURI());
        final Path parentPath = jarPath.getParent();
        truststorePath = parentPath.resolve(TRUSTSTORE_FILENAME);
        createTestTrustStore(truststorePath, TRUSTSTORE_PASSWORD);
    }

    @AfterEach
    void tearDown() throws IOException {
        // Clean up the test truststore file after each test
        if (truststorePath != null && truststorePath.toFile().exists()) {
            truststorePath.toFile().delete();
        }
    }

    @Test
    void testHttpClientWithTrustStoreConfiguration() {
        final var config = new HttpSinkConfig(Map.of(
            "http.url", "https://example.com",
            "http.authorization.type", "none",
            "http.ssl.truststore.location", TRUSTSTORE_FILENAME,
            "http.ssl.truststore.password", TRUSTSTORE_PASSWORD
        ));

        final var httpClient = HttpSenderFactory.buildHttpClient(config);
        assertThat(httpClient).isNotNull();
    }

    private void createTestTrustStore(final Path truststorePath, final String password) throws Exception {
        final KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        final KeyPair keyPair = keyGen.generateKeyPair();

        final X500Principal subject = new X500Principal("CN=Test,OU=Test,O=Test,C=US");
        final var certBuilder = new JcaX509v3CertificateBuilder(
            subject,
            BigInteger.valueOf(1),
            new Date(),
            new Date(System.currentTimeMillis() + 365L * 24 * 60 * 60 * 1000),
            subject,
            keyPair.getPublic()
        );

        final var signer = new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate());
        final var certHolder = certBuilder.build(signer);
        final X509Certificate cert = new JcaX509CertificateConverter().getCertificate(certHolder);

        final KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setCertificateEntry("testcert", cert);

        try (FileOutputStream fos = new FileOutputStream(truststorePath.toFile())) {
            keyStore.store(fos, password.toCharArray());
        }
    }

    @Test
    void testHttpClientWithoutSslConfiguration() {
        final var config = new HttpSinkConfig(Map.of(
            "http.url", "http://example.com",
            "http.authorization.type", "none"
        ));

        final var httpClient = HttpSenderFactory.buildHttpClient(config);
        assertThat(httpClient).isNotNull();
    }
}
