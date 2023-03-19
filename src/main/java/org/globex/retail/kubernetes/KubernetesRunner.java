package org.globex.retail.kubernetes;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Base64;

@ApplicationScoped
public class KubernetesRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesRunner.class);

    @Inject
    KubernetesClient client;

    public int run() {

        String kafkaNamespace = System.getenv("KAFKA_NAMESPACE");
        String kafkaClientConnectionSecret = System.getenv().getOrDefault("KAFKA_CLIENT_CONNECTION_SECRET", "kafka-client-secret");
        if (kafkaNamespace == null || kafkaNamespace.isBlank()) {
            LOGGER.error("Environment variable 'KAFKA_NAMESPACE' for kafka namespace not set. Exiting...");
            return -1;
        }

        String namespace = System.getenv("NAMESPACE");
        if (namespace == null || namespace.isBlank()) {
            LOGGER.error("Environment variable 'NAMESPACE' for namespace not set. Exiting...");
            return -1;
        }

        String clientSecret = System.getenv().getOrDefault("CLIENT_SECRET", "client-kafka");

        Secret kafkaSecret = client.secrets().inNamespace(kafkaNamespace).withName(kafkaClientConnectionSecret).get();
        if (kafkaSecret == null) {
            LOGGER.error("Secret " + kafkaClientConnectionSecret + " not found in namespace " + kafkaClientConnectionSecret);
            return -1;
        }

        String bootstrapServer = new String(Base64.getDecoder().decode(kafkaSecret.getData().get("bootstrapServer")));
        String securityProtocol = new String(Base64.getDecoder().decode(kafkaSecret.getData().get("securityProtocol")));
        String saslMechanism = new String(Base64.getDecoder().decode(kafkaSecret.getData().get("saslMechanism")));
        String saslJaasConfig = new String(Base64.getDecoder().decode(kafkaSecret.getData().get("saslJaasConfig")));

        String secretData = """
                camel.component.kafka.brokers=%s
                camel.component.kafka.securityprotocol=%s
                camel.component.kafka.saslmechanism=%s
                camel.component.kafka.sasljaasconfig=%s
                """.formatted(bootstrapServer, securityProtocol, saslMechanism, saslJaasConfig);

        Secret newSecret = new SecretBuilder().withNewMetadata().withName(clientSecret).endMetadata()
                .addToData("kafka.properties", Base64.getEncoder().encodeToString(secretData.getBytes())).build();
        client.secrets().inNamespace(namespace).resource(newSecret).createOrReplace();

        LOGGER.info("Secret " + clientSecret + " created in namespace " + namespace + ". Exiting.");

        return 0;
    }
}
