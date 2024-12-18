package com.economy.community.service;

import com.economy.community.repository.PostCacheRepository;
import com.economy.community.repository.PostRepository;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class PostViewCountBatchService {
    private final StringRedisTemplate stringRedisTemplate;
    private final PostRepository postRepository;
    private final PostCacheRepository postCacheRepository;

    @Transactional
    @Scheduled(fixedRate = 60000) // 매 1분마다 실행
    public void syncViewCountsToDatabase() {
        System.out.println("PostViewCountBatchService 돌아가는 중임...");

        // KEYS O(N) 블로킹 대신 SCAN 커서 방식으로 전환
        List<String> keys = new ArrayList<>();
        ScanOptions options = ScanOptions.scanOptions()
                .match("posts-cache::*::views")
                .count(100)
                .build();

        try (Cursor<byte[]> cursor = stringRedisTemplate.getConnectionFactory()
                .getConnection().scan(options)) {
            while (cursor.hasNext()) {
                keys.add(new String(cursor.next(), StandardCharsets.UTF_8));
            }
        }

        for (String key : keys) {
            Long postId = extractPostIdFromKey(key);
            // getAndSet으로 읽기+초기화를 원자적으로 수행하여 조회수 유실 방지
            // StringRedisTemplate 사용으로 "0" plain string 저장 → 다음 INCR 정상 동작
            String rawValue = stringRedisTemplate.opsForValue().getAndSet(
                    postCacheRepository.generateViewCacheKey(postId), "0");
            if (rawValue != null) {
                long increment = Long.parseLong(rawValue);
                if (increment > 0) {
                    postRepository.updatePostViewCount(postId, increment);
                }
            }
        }
    }

    // 키에서 postId를 추출하는 메서드
    private Long extractPostIdFromKey(String key) {
        // posts-cache::[postId]::views -> postId 추출
        String[] parts = key.split("::");
        return Long.parseLong(parts[1]);
    }
}
