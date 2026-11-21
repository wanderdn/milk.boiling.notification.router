package org.milk.boiling.notification.router.loadtest;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.web.reactive.config.WebFluxConfigurer;

@Configuration
@Profile("load-test")
public class LoadTestCodecConfiguration implements WebFluxConfigurer {
    @Override
    public void configureHttpMessageCodecs(ServerCodecConfigurer configurer) {
        // Benchmark batches must not hit the default 256 KiB JSON request limit.
        configurer.defaultCodecs().maxInMemorySize(4 * 1024 * 1024);
    }
}
