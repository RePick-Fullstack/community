package com.economy.community.service;

import com.economy.community.domain.CommentEvent;
import com.economy.community.domain.CommunityCategory;
import com.economy.community.domain.Post;
import com.economy.community.domain.PostLike;
import com.economy.community.dto.CreatePostRequest;
import com.economy.community.dto.CreatePostResponse;
import com.economy.community.dto.GetMyLikedPostResponse;
import com.economy.community.dto.PostLikesResponse;
import com.economy.community.dto.PostResponse;
import com.economy.community.dto.UpdatePostRequest;
import com.economy.community.dto.UpdatePostResponse;
import com.economy.community.repository.PostCacheRepository;
import com.economy.community.repository.PostLikeRepository;
import com.economy.community.repository.PostRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class PostServiceImpl implements PostService {

    private final ObjectMapper objectMapper;
    private final KafkaService kafkaService;
    private final RedissonClient redissonClient;

    private final PostRepository postRepository;
    private final PostLikeRepository postLikeRepository;
    private final PostCacheRepository postCacheRepository;

    @Override
    @Transactional(readOnly = true)
    public List<PostResponse> getPosts(String category, int page, int size) {
        return postRepository.findAllPosts(category, page, size);
    }

    @Override
    @Transactional(readOnly = true)
    public List<PostResponse> getMyPosts(Long userId) {
        return postRepository.getMyPosts(userId);
    }


    @Override
    @Transactional(readOnly = true)
    public PostResponse getPostById(Long id) {
        Post post = postRepository.findPostById(id);
        return PostResponse.from(post);
    }


    @Override
    @Transactional
    public CreatePostResponse createPost(CreatePostRequest request, Long userId, String userNickname) {
        Post post = Post.builder()
                .title(request.getTitle())
                .content(request.getContent())
                .category(CommunityCategory.valueOf(request.getCategory()))
                .userId(userId)
                .userNickname(userNickname)
                .likesCount(0L)
                .viewCount(0L)
                .commentsCount(0L)
                .build();
        Post savedPost = postRepository.save(post);

        // Response 반환
        Long likeCount = 0L;
        return new CreatePostResponse(savedPost, likeCount);
    }

    @Override
    @Transactional
    public UpdatePostResponse updatePost(UpdatePostRequest request, Long id, Long userId) {
        Post post = postRepository.findPostById(id);

        // 사용자 검증
        post.validateUserAuthorization(userId);

        // 게시글 수정 (불변 객체 방식)
        Post updatedPost = post.withUpdatedFields(request.getTitle(), request.getContent());
        Post savedPost = postRepository.save(updatedPost);

        // 좋아요 수를 Redis에서 조회
        Long likeCount = postCacheRepository.getLikeCount(savedPost.getId());

        return new UpdatePostResponse(savedPost, likeCount);
    }

    @Override
    @Transactional
    public void deletePost(Long id, Long userId) {
        Post post = postRepository.findPostById(id);

        // 사용자 검증
        post.validateUserAuthorization(userId);

        // 게시글 삭제 (불변 객체 방식)
        Post deletedPost = post.withDeleted();
        postRepository.save(deletedPost);
    }

    @Override
    @Transactional
    public PostLikesResponse toggleLike(Long id, Long userId, String userNickname) {
        String lockKey = "lock:like:" + userId + ":" + id;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            if (!lock.tryLock(3, 10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("잠시 후 다시 시도해주세요.");
            }

            // 게시글 조회
            Post post = postRepository.findPostById(id);

            if (post.getUserId().equals(userId)) {
                throw new IllegalArgumentException("You cannot like your own post.");
            }

            // 좋아요 여부 확인
            Optional<PostLike> existingLike = postLikeRepository.findByUserIdAndPostId(userId, post.getId());

            boolean isLiked;

            if (existingLike.isEmpty()) {
                // 중복 방지: Redisson 분산락 1차 방어 + 유니크 제약 조건 2차 방어
                PostLike newLike = PostLike.builder()
                        .userId(userId)
                        .userNickname(userNickname)
                        .post(post)
                        .build();
                postLikeRepository.save(newLike);

                postCacheRepository.incrementLikeCount(id); // Redis에서 좋아요 수 증가
                isLiked = true;

                // **좋아요 알림 생성**
                createLikeNotification(post, userNickname);

            } else {
                // 좋아요 취소
                PostLike like = existingLike.get();
                postLikeRepository.delete(like);

                postCacheRepository.decrementLikeCount(id); // Redis에서 좋아요 수 감소
                isLiked = false;
            }

            // Redis에서 최신 좋아요 수 가져오기
            Long updatedLikeCount = postCacheRepository.getLikeCount(id);

            // 좋아요 수 동기화
            post.syncLikesCount(updatedLikeCount);
            postRepository.save(post);

            return new PostLikesResponse(isLiked, updatedLikeCount);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("락 대기 중 인터럽트 발생", e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void createLikeNotification(Post post, String likerNickname) {
        // 게시글 작성자에게 알림 생성 (자신이 자신의 게시글에 좋아요를 누른 경우 제외)
        if (!post.getUserId().equals(post.getId())) {
            String notificationMessage = likerNickname + "님이 내 게시글에 좋아요를 눌렀습니다.";
            String notificationDetails = "게시글 제목: " + post.getTitle() + "\n게시글 내용: " + post.getContent();
            postCacheRepository.addNotification(post.getUserId(), notificationMessage, post.getId(),
                    notificationDetails);
        }
    }

    @Override
    @Transactional
    public List<GetMyLikedPostResponse> getMyLikedPosts(Long userId) {
        List<PostLike> likedPosts = postLikeRepository.findByUserId(userId);

        return likedPosts.stream()
                .map(like -> new GetMyLikedPostResponse(
                        like.getPost().getId(),
                        like.getPost().getContent(),
                        like.getPost().getLikesCount()
                ))
                .collect(Collectors.toList());
    }

    @KafkaListener(topics = "comment-topic", groupId = "post-service-group")
    public void handleCommentEvent(byte[] message) {
        try {
            CommentEvent event = objectMapper.readValue(message, CommentEvent.class);

            // 댓글 생성 이벤트 처리
            if ("CREATE".equals(event.getAction())) {
                kafkaService.incrementCommentsCount(event.getPostId());

                // 댓글 알림 생성
                createCommentNotification(event);

            }
            // 댓글 삭제 이벤트 처리
            else if ("DELETE".equals(event.getAction())) {
                kafkaService.decrementCommentsCount(event.getPostId());
            }
        } catch (Exception e) {
            throw new RuntimeException("Kafka 메시지 처리 실패", e);
        }
    }

    private void createCommentNotification(CommentEvent event) {
        // 게시글 조회
        Post post = postRepository.findPostById(event.getPostId());
        if (post == null) {
            throw new IllegalArgumentException("게시글이 존재하지 않습니다.");
        }

        // 댓글 작성 알림 생성
        String notificationMessage = event.getUserNickname() + "님이 내 게시글에 댓글을 작성했습니다.";
        String notificationDetails = "댓글 내용: " + event.getContent();
        postCacheRepository.addNotification(post.getUserId(), notificationMessage, post.getId(), notificationDetails);
    }

}
