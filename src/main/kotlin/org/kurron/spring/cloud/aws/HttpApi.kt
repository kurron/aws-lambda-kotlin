package org.kurron.spring.cloud.aws

import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/article")
class ArticleController() {

    @GetMapping("/")
    fun findAll() = listOf<Article>()

}
