package org.milk.boiling.notification.router;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;

import java.util.Map;

@Configuration
@Slf4j
public class RouterHandlerMappingConfig {

    @Bean
    public HandlerMapping handlerMapping(UserWebSocketHandler userWebSocketHandler) {
        var handlerMapping = new SimpleUrlHandlerMapping();
        handlerMapping.setOrder(1);
        handlerMapping.setUrlMap(Map.of(userWebSocketHandler.getPath(), userWebSocketHandler));

        return handlerMapping;
    }
}
