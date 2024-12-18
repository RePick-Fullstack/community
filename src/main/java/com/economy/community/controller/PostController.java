package com.economy.community.controller;

import com.economy.community.dto.CreatePostRequest;
import com.economy.community.dto.CreatePostResponse;
import com.economy.community.dto.GetMyLikedPostResponse;
import com.economy.community.dto.PostLikesResponse;
import com.economy.community.dto.PostResponse;
import com.economy.community.dto.UpdatePostRequest;
import com.economy.community.dto.UpdatePostResponse;
import com.economy.community.jwt.TokenProvider;
import com.economy.community.service.PostService;
import com.economy.community.service.PostViewCountService;
import jakarta.persistence.Table;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Table(name = "POST")
@CrossOrigin("*")
@RestController
@RequestMapping("/api/v1/posts")
@RequiredArgsConstructor
public class PostController {

    private final PostService service;
    private final TokenProvider tokenProvider;
    private final PostViewCountService postViewCountService;

    @GetMapping
    public List<PostResponse> getPosts(
            @RequestParam(required = false) String category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {

        return service.getPosts(category, page, size);
    }

    @GetMapping("/users/me")
    public List<PostResponse> getMyPosts(@RequestHeader String Authorization) {
        Long userId = tokenProvider.getUserIdFromToken(Authorization);
        return service.getMyPosts(userId);
    }

    @GetMapping("/{id}")
    public PostResponse getPostById(@PathVariable Long id) {
        return service.getPostById(id);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CreatePostResponse createCommunity(@RequestBody @Valid CreatePostRequest request,
                                              @RequestHeader String Authorization) {
        Long userId = tokenProvider.getUserIdFromToken(Authorization);
        String userNickname = tokenProvider.getNickNameFromToken(Authorization);

        return service.createPost(request, userId, userNickname);
    }

    @PutMapping("/{id}")
    public UpdatePostResponse updateCommunity(@RequestBody UpdatePostRequest request,
                                              @PathVariable Long id,
                                              @RequestHeader String Authorization) {
        Long userId = tokenProvider.getUserIdFromToken(Authorization);
        return service.updatePost(request, id, userId);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void deleteCommunity(@PathVariable Long id, @RequestHeader String Authorization) {
        Long userId = tokenProvider.getUserIdFromToken(Authorization);
        service.deletePost(id, userId);
    }

    // 게시글 좋아요
    @PostMapping("/{id}/like")
    public ResponseEntity<PostLikesResponse> likePost(@PathVariable Long id,
                                                      @RequestHeader String Authorization) {
        Long userId = tokenProvider.getUserIdFromToken(Authorization);
        String userNickname = tokenProvider.getNickNameFromToken(Authorization);

        PostLikesResponse response = service.toggleLike(id, userId, userNickname);
        return ResponseEntity.ok(response);
    }

    // 게시글 좋아요 목록 조회(마이페이지)
    @GetMapping("/users/me/like")
    public ResponseEntity<List<GetMyLikedPostResponse>> getMyLikedPosts(@RequestHeader String Authorization) {
        Long userId = tokenProvider.getUserIdFromToken(Authorization);

        List<GetMyLikedPostResponse> likedPosts = service.getMyLikedPosts(userId);

        return ResponseEntity.ok(likedPosts);
    }

    // 게시글 조회수 증가 (userId 기반 중복 조회 차단)
    @GetMapping("/{id}/view")
    public ResponseEntity<Long> postView(@PathVariable Long id,
                                         @RequestHeader String Authorization) {
        Long userId = tokenProvider.getUserIdFromToken(Authorization);
        Long updatedViewCount = postViewCountService.incrementPostViewCount(id, userId);
        return ResponseEntity.ok(updatedViewCount);
    }

    // 게시글 조회수 가져오기
    @GetMapping("/{id}/viewCount")
    public ResponseEntity<Long> getPostViewCount(@PathVariable Long id) {
        Long viewCount = postViewCountService.getPostViewCount(id);
        return ResponseEntity.ok(viewCount);
    }
}
