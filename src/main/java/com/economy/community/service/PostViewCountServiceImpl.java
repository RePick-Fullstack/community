package com.economy.community.service;

import com.economy.community.repository.PostCacheRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PostViewCountServiceImpl implements PostViewCountService {

    private final PostCacheRepository postCacheRepository;

    @Override
    public Long incrementPostViewCount(Long id, Long userId) {
        // userId TTL 키 기반 중복 차단 Lua Script 실행 (중복 요청은 0 반환)
        postCacheRepository.incrementViewCount(id, userId);
        return postCacheRepository.getViewCount(id);
    }

    @Override
    public Long getPostViewCount(Long id) {
        return postCacheRepository.getViewCount(id);
    }
}
