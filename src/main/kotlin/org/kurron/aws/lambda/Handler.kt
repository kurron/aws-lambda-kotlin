package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SNSEvent
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper

/**
 * AWS Lambda entry point.
 */
class Handler: RequestHandler<SNSEvent,Unit> {
    override fun handleRequest(input: SNSEvent, context: Context) {
        val mapper = ObjectMapper()
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

        input.records.forEach { snsRecord ->
            val json = snsRecord.sns.message
            context.logger.log("message = $json")
            val event = mapper.readValue<S3Event>( json, object : TypeReference<S3Event>() {})
            event.records.forEach { s3Record ->
                val region = s3Record.region
                val bucket = s3Record.record.bucket.name
                val key = s3Record.record.data.key
                context.logger.log( "region = $region, bucket = $bucket, key = $key" )
            }

        }
    }
}