package com.economy.community.service;

import com.economy.community.domain.Notification;
import com.economy.community.domain.NotificationEntity;
import com.economy.community.repository.NotificationRepository;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.request.async.DeferredResult;

@Service
@RequiredArgsConstructor
public class NotificationService {

    private final NotificationRepository notificationRepository;

    private final ConcurrentHashMap<Long, DeferredResult<List<Notification>>> pendingRequests =
            new ConcurrentHashMap<>();

    public void waitForUserNotifications(Long userId, DeferredResult<List<Notification>> deferredResult) {
        // Redis publish 실패·서버 재시작 후 재요청 시 DB에서 즉시 반환
        List<NotificationEntity> existing = notificationRepository.findByUserId(userId);
        if (!existing.isEmpty()) {
            deferredResult.setResult(toDtos(existing));
            return;
        }

        pendingRequests.put(userId, deferredResult);
        deferredResult.onTimeout(() -> {
            pendingRequests.remove(userId);
            deferredResult.setResult(List.of());
        });
        deferredResult.onCompletion(() -> pendingRequests.remove(userId));
    }

    @Transactional(readOnly = true)
    public void onNotificationReceived(Long userId) {
        DeferredResult<List<Notification>> dr = pendingRequests.remove(userId);
        if (dr == null || dr.isSetOrExpired()) {
            return;
        }
        dr.setResult(toDtos(notificationRepository.findByUserId(userId)));
    }

    @Transactional
    public void clearUserNotifications(Long userId) {
        notificationRepository.deleteByUserId(userId);
    }

    private List<Notification> toDtos(List<NotificationEntity> entities) {
        return entities.stream()
                .map(NotificationEntity::toNotification)
                .collect(Collectors.toList());
    }
}
