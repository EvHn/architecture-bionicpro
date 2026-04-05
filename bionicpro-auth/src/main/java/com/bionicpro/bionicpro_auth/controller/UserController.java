package com.bionicpro.bionicpro_auth.controller;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class UserController {

    @GetMapping("/api/me")
    public Map<String, Object> getCurrentUser() {
        var auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated()) {
            return Map.of("authenticated", false);
        }
        // Здесь можно извлечь атрибуты из OAuth2User
        OAuth2User oauth2User = (OAuth2User) auth.getPrincipal();
        return Map.of(
                "authenticated", true,
                "name", oauth2User.getAttribute("preferred_username"),
                "email", oauth2User.getAttribute("email"),
                "roles", oauth2User.getAuthorities().stream()
                        .map(GrantedAuthority::getAuthority)
                        .toList()
        );
    }
}