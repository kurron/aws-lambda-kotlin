package org.kurron.spring.cloud.aws

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.junit.jupiter.SpringExtension
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@ExtendWith(SpringExtension::class)
@WebMvcTest
class HttpApiTests(@Autowired val mockMvc: MockMvc) {

    @Test
    fun `List articles`() {
        mockMvc.perform(get("/api/article/")
               .accept(MediaType.APPLICATION_JSON))
               .andExpect(status().isOk)
    }

}