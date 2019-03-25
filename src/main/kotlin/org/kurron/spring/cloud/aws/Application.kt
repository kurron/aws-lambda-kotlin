package org.kurron.spring.cloud.aws

import com.samskivert.mustache.Mustache
import org.springframework.boot.Banner.Mode.OFF
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean

@SpringBootApplication
@EnableConfigurationProperties(ConfigurationProperties::class)
class BlogApplication {
    @Bean
    fun mustacheCompiler( loader: Mustache.TemplateLoader? ) = Mustache.compiler().escapeHTML( false ).withLoader( loader )!!

}

fun main(args: Array<String>) {
    runApplication<BlogApplication>(*args) {
        setBannerMode(OFF)
    }
}
