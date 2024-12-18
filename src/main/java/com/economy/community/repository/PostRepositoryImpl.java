package com.economy.community.repository;

import com.economy.community.domain.CommunityCategory;
import com.economy.community.domain.Post;
import com.economy.community.domain.QPost;
import com.economy.community.dto.PostResponse;
import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Projections;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.querydsl.jpa.impl.JPAUpdateClause;
import jakarta.persistence.EntityManager;
import java.util.List;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PostRepositoryImpl implements PostRepositoryCustom {

    private final JPAQueryFactory queryFactory;
    private final PostCacheRepository postCacheRepository;
    private final EntityManager entityManager;

    @Override
    public List<PostResponse> findAllPosts(String category, int page, int size) {
        QPost post = QPost.post;

        BooleanBuilder builder = new BooleanBuilder();

        // 동적 조건: 삭제되지 않은 게시글
        builder.and(post.deleted.eq(false));

        if (category != null) {
            CommunityCategory categoryEnum = CommunityCategory.valueOfCategory(category);
            builder.and(post.category.eq(categoryEnum));
        }

        List<PostResponse> posts = queryFactory
                .select(Projections.constructor(PostResponse.class,
                        post.id,
                        post.category,
                        post.title,
                        post.content,
                        post.userId,
                        post.userNickname,
                        post.createdAt,
                        post.likesCount,
                        post.viewCount,
                        post.commentsCount))
                .from(post)
                .where(builder)
                .orderBy(post.createdAt.desc())
                .offset((long) page * size)
                .limit(size)
                .fetch();

        // Redis에서 좋아요 수를 조회하여 업데이트
        posts.forEach(p -> {
            Long redisLikeCount = postCacheRepository.getLikeCount(p.getId());
            p.withUpdatedLikesCount(redisLikeCount); // 좋아요 수 업데이트

            // Redis에서 조회수 조회하여 업데이트
            Long redisViewCount = postCacheRepository.getViewCount(p.getId());
            if (redisViewCount != null) {
                p.withUpdatedViewCount(redisViewCount); // 조회수 업데이트
            }
        });

        return posts;
    }

    @Override
    public Post findPostById(Long id) {
        QPost post = QPost.post;

        Post result = queryFactory
                .selectFrom(post)
                .where(post.id.eq(id).and(post.deleted.eq(false)))
                .fetchOne();

        if (result == null) {
            throw new IllegalArgumentException("Post not found with id: " + id);
        }

        // Redis에서 좋아요 수를 조회하고 동기화 (Optional)
        Long redisLikeCount = postCacheRepository.getLikeCount(result.getId());
        result.syncLikesCount(redisLikeCount); // 좋아요 수 동기화

        // Redis에서 조회수 조회하고 동기화
        Long redisViewCount = postCacheRepository.getViewCount(result.getId());
        result.syncViewCount(redisViewCount); // 조회수 동기화

        return result;
    }

    @Override
    public List<PostResponse> getMyPosts(Long userId) {
        QPost post = QPost.post;

        List<PostResponse> posts = queryFactory
                .select(Projections.constructor(PostResponse.class,
                        post.id,
                        post.category,
                        post.title,
                        post.content,
                        post.userId,
                        post.userNickname,
                        post.createdAt,
                        post.likesCount, // DB의 좋아요 수
                        post.viewCount,
                        post.commentsCount))
                .from(post)
                .where(post.userId.eq(userId).and(post.deleted.eq(false)))
                .fetch();

        // Redis에서 좋아요 수를 조회하여 업데이트
        posts.forEach(p -> {
            Long redisLikeCount = postCacheRepository.getLikeCount(p.getId());
            p.withUpdatedLikesCount(redisLikeCount); // 좋아요 수 업데이트
        });

        return posts;
    }

    @Override
    public void updatePostViewCount(Long id, int increment) {
        QPost post = QPost.post;

        new JPAUpdateClause(entityManager, post)
                .set(post.viewCount, post.viewCount.add(increment))
                .where(post.id.eq(id))
                .execute();
    }
}
