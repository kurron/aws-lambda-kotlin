package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.S3Event
import com.amazonaws.services.lambda.runtime.events.SNSEvent
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.PublishRequest
import java.lang.management.ManagementFactory
import java.util.*

/**
 * This AWS Lambda will use the name of the file just uploaded to S3 as the routing key to SNS.  This allows the message to get routed to the proper SQS queue.
 */
class CsvRouter: RequestHandler<S3Event, Unit> {
    override fun handleRequest(input: S3Event, context: Context) {
        dumpJvmSettings(context)

        val jsonMapper = createJsonMapper()
        val sns = SnsClient.builder().build()

        context.logger.log( "message: ${input}")
    }

    private fun publishMessage(mapper: ObjectMapper, data: SkuProductRowHolder, context: Context, sns: SnsClient) {
        val message = mapper.writeValueAsString(data)
        context.logger.log("Submitting a message with : ${message.length} characters")
        val topicArn: String = Optional.ofNullable(System.getenv("TOPIC_ARN")).orElseThrow { IllegalStateException("TOPIC_ARN was not provided!") }
        val request = PublishRequest.builder().topicArn(topicArn).message(message).build()
        try {
            sns.publish(request)
        } catch (e: Exception) {
            context.logger.log( "Unable to publish message: ${e.message}" )
        }
    }

    private fun dumpJvmSettings(context: Context) {
        val runtimeMxBean = ManagementFactory.getRuntimeMXBean()
        val arguments = runtimeMxBean.inputArguments
        arguments.forEach {
            context.logger.log(it)
        }
    }

    private fun toS3Event(snsRecord: SNSEvent.SNSRecord, context: Context, jsonMapper: ObjectMapper): S3Message {
        val json = snsRecord.sns.message
        context.logger.log("message = $json")
        return jsonMapper.readValue(json)
    }

    private fun createJsonMapper(): ObjectMapper {
        val mapper = ObjectMapper().registerModule(KotlinModule())
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        return mapper
    }
}