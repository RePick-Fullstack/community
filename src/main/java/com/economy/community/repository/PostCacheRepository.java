package com.economy.community.repository;

import com.economy.community.domain.Notification;
import com.economy.community.dto.PostResponse;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class PostCacheRepository implements CacheRepository {

    public static final String CACHE_KEY = "posts-cache";

    private static final String RANKING_KEY = "ranking:views";

    private static final String SAFE_DECR_SCRIPT =
            "local current = redis.call('get', KEYS[1]) " +
            "if current and tonumber(current) > 0 then " +
            "    return redis.call('decr', KEYS[1]) " +
            "end " +
            "return 0";

    // userId TTL 키 존재 시 중복 조회로 판단, 없으면 TTL 설정 후 조회수·랭킹 ZSET 원자적 갱신
    private static final String VIEW_COUNT_SCRIPT =
            "if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end " +
            "redis.call('SET', KEYS[1], '1') " +
            "redis.call('EXPIRE', KEYS[1], ARGV[1]) " +
            "redis.call('INCR', KEYS[2]) " +
            "redis.call('ZINCRBY', KEYS[3], 1, ARGV[2]) " +
            "return 1";

    private final RedisTemplate<String, Object> redisTemplate;
    private final StringRedisTemplate stringRedisTemplate;

    // 사용자별 알림 리스트를 관리하는 맵
    private final Map<Long, List<Notification>> notifications = new ConcurrentHashMap<>();

    @Override
    public String getCacheKey() {
        return CACHE_KEY;
    }

    @Override
    public List<PostResponse> getCacheDate(String cacheKey) {
        return (List<PostResponse>) redisTemplate.opsForValue().get(cacheKey);
    }

    @Override
    public void saveCacheData(String cacheKey, Object data) {
        redisTemplate.opsForValue().set(cacheKey, data);
    }

    @Override
    public void deleteCacheData(String cacheKey) {
        redisTemplate.delete(cacheKey);
    }

    // 좋아요 캐시 키 생성
    public String generateLikeCacheKey(Long postId) {
        return String.format("%s::%d::likes", CACHE_KEY, postId);
    }

    // 좋아요 수 가져오기
    public Long getLikeCount(Long postId) {
        String cacheKey = generateLikeCacheKey(postId);
        Object cachedValue = redisTemplate.opsForValue().get(cacheKey);

        if (cachedValue != null) {
            try {
                // Object를 String으로 변환 후 Long으로 변환
                return Long.valueOf(cachedValue.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Redis에 저장된 좋아요 데이터가 숫자가 아닙니다: " + cachedValue);
            }
        }

        return 0L;
    }

    // 좋아요 수 증가
    public void incrementLikeCount(Long postId) {
        String cacheKey = generateLikeCacheKey(postId);
        redisTemplate.opsForValue().increment(cacheKey);
    }

    // 좋아요 수 감소 (Lua 스크립트로 음수 방지)
    public void decrementLikeCount(Long postId) {
        String cacheKey = generateLikeCacheKey(postId);
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(SAFE_DECR_SCRIPT, Long.class);
        redisTemplate.execute(script, Collections.singletonList(cacheKey));
    }

    // 조회수 캐시 키 생성
    public String generateViewCacheKey(Long postId) {
        return String.format("%s::%d::views", CACHE_KEY, postId);
    }

    // userId 기반 중복 조회 차단 키 생성
    public String generateUserViewKey(Long postId, Long userId) {
        return String.format("view:user:%d:post:%d", userId, postId);
    }

    // 조회수 증가 (userId TTL 키로 중복 차단, Lua Script로 원자적 처리)
    // StringRedisTemplate 사용: plain string 저장으로 INCR 타입 오류 방지
    public Long incrementViewCount(Long postId, Long userId) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(VIEW_COUNT_SCRIPT, Long.class);
        return stringRedisTemplate.execute(
                script,
                Arrays.asList(
                        generateUserViewKey(postId, userId),
                        generateViewCacheKey(postId),
                        RANKING_KEY
                ),
                "86400",          // userId TTL: 24시간
                postId.toString() // ZINCRBY member
        );
    }

    // 조회수 가져오기 (StringRedisTemplate으로 plain string 파싱)
    public Long getViewCount(Long postId) {
        String value = stringRedisTemplate.opsForValue().get(generateViewCacheKey(postId));
        if (value == null) {
            return 0L;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Redis에 저장된 조회수 데이터가 숫자가 아닙니다: " + value);
        }
    }

    // 알림 저장
    public void addNotification(Long userId, String message, Long postId, String details) {
        Notification notification = new Notification(postId, message, details, LocalDateTime.now());
        notifications.computeIfAbsent(userId, k -> Collections.synchronizedList(new ArrayList<>())).add(notification);
    }

    // 사용자별 알림 조회
    public List<Notification> getNotifications(Long userId) {
        return notifications.getOrDefault(userId, new ArrayList<>());
    }

    // 사용자별 알림 삭제
    public void deleteNotifications(Long userId) {
        notifications.remove(userId);
    }
}
