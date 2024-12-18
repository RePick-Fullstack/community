package com.economy.community.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationEventListener {

    private final StringRedisTemplate stringRedisTemplate;

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onNotificationCreated(NotificationCreatedEvent event) {
        try {
            stringRedisTemplate.convertAndSend("notification:user:" + event.getUserId(), "new");
        } catch (Exception e) {
            // publish 실패는 알림 유실이 아님 — 클라이언트 재요청 시 DB-first 조회로 수신
            log.warn("Redis publish 실패 (userId={}): {}", event.getUserId(), e.getMessage());
        }
    }
}
