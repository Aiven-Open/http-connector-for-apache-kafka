module io.aiven.kafka.connect.http {
    // http
    requires java.net.http;
    requires com.fasterxml.jackson.databind;
    // kafka
    requires static kafka.clients;
    requires static connect.api;
    requires connect.json;
    // tooling
    requires org.slf4j;
    requires com.github.spotbugs.annotations;
    requires json.path;
}
