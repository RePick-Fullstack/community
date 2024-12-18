package com.economy.community.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Post extends BaseEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "post_id")
    private Long id;

    @Column(name = "user_id")
    @NotNull
    private Long userId;

    @Column(name = "user_nickname")
    @NotNull
    private String userNickname;

    @Column(name = "post_title")
    @NotNull
    private String title;

    @Column(name = "post_content", columnDefinition = "TEXT")
    private String content;

    @Column(name = "community_category")
    @Enumerated(EnumType.STRING)
    private CommunityCategory category;

    @Builder.Default
    @Column(name = "like_count")
    private Long likesCount = 0L;

    @Builder.Default
    @Column(name = "view_count")
    private Long viewCount = 0L;

    @Builder.Default
    @Column(name = "comment_count")
    private Long commentsCount = 0L;

    @Builder.Default
    @Column(name = "deleted")
    @NotNull
    private boolean deleted = false;


    public Post withUpdatedFields(String title, String content) {
        return Post.builder()
                .id(this.id)
                .userId(this.userId)
                .userNickname(this.userNickname)
                .title(title)  // 새로운 제목으로 설정
                .content(content)  // 새로운 내용으로 설정
                .category(this.category)
                .likesCount(this.likesCount)
                .viewCount(this.viewCount)
                .commentsCount(this.commentsCount)
                .deleted(this.deleted)
                .build();
    }

    public Post withDeleted() {

        return Post.builder()
                .id(this.id)
                .userId(this.userId)
                .userNickname(this.userNickname)
                .title(this.title)
                .content(this.content)
                .category(this.category)
                .likesCount(this.likesCount)
                .viewCount(this.viewCount)
                .commentsCount(this.commentsCount)
                .deleted(true)  // 삭제 상태로 설정
                .build();
    }

    public void syncLikesCount(Long cachedLikesCount) {
        this.likesCount = cachedLikesCount;
    }

    public void syncViewCount(Long cachedViewCount) {
        this.viewCount = cachedViewCount;
    }

    // 댓글 추가 시 호출
    public void incrementCommentsCount() {
        this.commentsCount++;
    }

    // 댓글 삭제 시 호출
    public void decrementCommentsCount() {
        if (this.commentsCount > 0) {
            this.commentsCount--;
        }
    }

    public void validateUserAuthorization(Long userId) {
        if (!this.userId.equals(userId)) {
            throw new IllegalArgumentException("You are not authorized to perform this action on this post");
        }
    }
}
