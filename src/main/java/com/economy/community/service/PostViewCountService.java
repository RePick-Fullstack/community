package com.economy.community.service;

public interface PostViewCountService {

    Long incrementPostViewCount(Long id, Long userId);

    Long getPostViewCount(Long id);
}
