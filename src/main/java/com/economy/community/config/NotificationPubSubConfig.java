package com.economy.community.config;

import com.economy.community.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
@RequiredArgsConstructor
public class NotificationPubSubConfig {

    private final RedisConnectionFactory redisConnectionFactory;
    private final NotificationService notificationService;

    @Bean
    public RedisMessageListenerContainer notificationListenerContainer() {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory);
        container.addMessageListener(
            (message, pattern) -> {
                String channel = new String(message.getChannel());
                Long userId = Long.parseLong(channel.substring("notification:user:".length()));
                notificationService.onNotificationReceived(userId);
            },
            new PatternTopic("notification:user:*")
        );
        return container;
    }
}
