package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.*
import com.amazonaws.services.lambda.runtime.events.S3Event
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
    override fun handleRequest(input: S3Event, context: Context) {
        dumpJvmSettings(context)

        val mapper = createJsonMapper()
        val sns = SnsClient.builder().build()

        input.records.forEach { record ->
            val routingKey = "${record.awsRegion}/${record.s3.bucket.name}/${record.s3.`object`.key}"
            context.logger.log( "Processing change event with ${record.awsRegion}/${record.s3.bucket.name}/${record.s3.`object`.key}")
            val event = S3ChangeEvent( region = record.awsRegion, bucket = record.s3.bucket.name, key = record.s3.`object`.key )
            val message = mapper.writeValueAsString(event)
            val topicArn: String = Optional.ofNullable(System.getenv("TOPIC_ARN"))
                                           .orElseThrow { IllegalStateException("TOPIC_ARN was not provided!") }
            val value = MessageAttributeValue.builder().dataType("String").stringValue(record.s3.`object`.key).build()
            val request = PublishRequest.builder()
                                                     .topicArn(topicArn)
                                                     .message(message)
                                                     .messageAttributes(hashMapOf( "routing-key" to value ))
                                                     .build()
            sns.publish(request)
            context.logger.log( "$routingKey event sent to SNS $topicArn")
        }
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