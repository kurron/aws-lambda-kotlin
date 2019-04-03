package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.MessageAttributeValue
import software.amazon.awssdk.services.sns.model.PublishRequest
import java.lang.management.ManagementFactory
import java.util.*

/**
 * Decomposes a batch of Alpha messages into individual messages.
 */
class AlphaBatchHandler: RequestHandler<SQSEvent, Unit> {
    private val mapper = createJsonMapper()
    private val sns = SnsClient.builder().build()
    private val topicArn: String = Optional.ofNullable(System.getenv("TOPIC_ARN")).orElseThrow { IllegalStateException("TOPIC_ARN was not provided!") }

    override fun handleRequest(input: SQSEvent, context: Context) {
        dumpJvmSettings(context)

        // TODO: see if using streams instead of loops is more readable and/or efficient
        input.records.forEach { sqsRecord ->
            context.logger.log( "Processing ${sqsRecord.body}")
            val routingKey = sqsRecord.messageAttributes.get("routing-key")?.stringValue ?: "routing key not provided"
            val holder = mapper.readValue<SkuProductRowHolder>(sqsRecord.body)
            holder.rows.forEach{ row ->
                val request = createRequest(row, mapper, topicArn, routingKey)
                val response = sns.publish(request)
                assert( response.sdkHttpResponse().isSuccessful )
                context.logger.log( "${response.messageId()} event sent to SNS $topicArn")
            }
        }
        context.logger.log( "Processing complete.")
    }

    private fun createRequest(row: SkuProductRow, mapper: ObjectMapper, topicArn: String, routingKey: String): PublishRequest {
        val message = mapper.writeValueAsString(row)
        val value = MessageAttributeValue.builder().dataType("String").stringValue(routingKey).build()
        return PublishRequest.builder()
                             .topicArn(topicArn)
                             .message(message)
                             .messageAttributes(hashMapOf("routing-key" to value))
                             .build()
    }

    private fun dumpJvmSettings(context: Context) {
        val runtimeMxBean = ManagementFactory.getRuntimeMXBean()
        val arguments = runtimeMxBean.inputArguments
        arguments.forEach {
            context.logger.log(it)
        }
    }

    private fun createJsonMapper(): ObjectMapper {
        val mapper = ObjectMapper().registerModule(KotlinModule())
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        return mapper
    }
}