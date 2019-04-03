package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.*
import com.amazonaws.services.lambda.runtime.events.S3Event
import com.amazonaws.services.s3.event.S3EventNotification
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.MessageAttributeValue
import software.amazon.awssdk.services.sns.model.PublishRequest
import java.lang.management.ManagementFactory
import java.util.*

/**
 * This AWS Lambda will use the name of the file just uploaded to S3 as the routing key to SNS.  This allows the message to get routed to the proper SQS queue.
 */
class CsvRouter: RequestHandler<S3Event, Unit> {
    private val mapper = createJsonMapper()
    private val sns = SnsClient.builder().build()
    private val topicArn: String = Optional.ofNullable(System.getenv("TOPIC_ARN")).orElseThrow { IllegalStateException("TOPIC_ARN was not provided!") }

    override fun handleRequest(input: S3Event, context: Context) {
        dumpJvmSettings(context)

        input.records.forEach { record ->
            val routingKey = "${record.awsRegion}/${record.s3.bucket.name}/${record.s3.`object`.key}"
            context.logger.log( "Processing change event with ${record.awsRegion}/${record.s3.bucket.name}/${record.s3.`object`.key}")
            val request = createRequest(record, mapper, topicArn)
            val response = sns.publish(request)
            assert( response.sdkHttpResponse().isSuccessful )
            context.logger.log( "$routingKey event sent to SNS $topicArn")
        }
    }

    private fun createRequest(record: S3EventNotification.S3EventNotificationRecord, mapper: ObjectMapper, topicArn: String): PublishRequest? {
        val message = toJSON(record, mapper)
        val value = MessageAttributeValue.builder().dataType("String").stringValue(record.s3.`object`.key).build()
        return PublishRequest.builder()
                             .topicArn(topicArn)
                             .message(message)
                             .messageAttributes(hashMapOf("routing-key" to value))
                             .build()
    }

    private fun toJSON(record: S3EventNotification.S3EventNotificationRecord, mapper: ObjectMapper): String {
        val event = S3ChangeEvent(region = record.awsRegion, bucket = record.s3.bucket.name, key = record.s3.`object`.key)
        return mapper.writeValueAsString(event)
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

fun main(args : Array<String>) {
    println( "Hello, World! args is ${args.size} long.")
}