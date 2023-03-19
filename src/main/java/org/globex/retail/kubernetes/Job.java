package org.globex.retail.kubernetes;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;

import javax.inject.Inject;

@QuarkusMain
public class Job implements QuarkusApplication {

    @Inject
    KubernetesRunner installer;

    @Override
    public int run(String... args) throws Exception {
        return installer.run();
    }
}
