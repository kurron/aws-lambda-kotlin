package org.kurron.spring.cloud.aws

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("blog")
class ConfigurationProperties {

    lateinit var title: String
    val banner = Banner()

    class Banner {
        var title: String? = null
        lateinit var content: String
    }
}