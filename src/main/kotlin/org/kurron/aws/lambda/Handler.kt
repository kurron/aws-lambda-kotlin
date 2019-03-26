package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SNSEvent
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvParser
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse

/**
 * AWS Lambda entry point.
 */
class Handler: RequestHandler<SNSEvent,Unit> {
    override fun handleRequest(input: SNSEvent, context: Context) {
        val jsonMapper = createJsonMapper()
        val foo = CsvMapper()
        foo.enable( CsvParser.Feature.SKIP_EMPTY_LINES )
        foo.enable( CsvParser.Feature.TRIM_SPACES )

        input.records.forEach { snsRecord ->
            val json = snsRecord.sns.message
            context.logger.log("message = $json")
            val event = jsonMapper.readValue<S3Event>( json, object : TypeReference<S3Event>() {})
            event.records.forEach { s3Record ->
                val region = s3Record.region
                val bucket = s3Record.record.bucket.name
                val key = s3Record.record.data.key
                context.logger.log( "region = $region, bucket = $bucket, key = $key" )

                val response = downloadFile(bucket, key)
                context.logger.log("Just download $bucket/$key which was ${response.response().contentLength()} bytes long.")
            }
        }
    }

    fun downloadFile(bucket: String, key: String): ResponseBytes<GetObjectResponse> {
        val s3 = S3Client.builder().build()
        val objectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build()
        val response = s3.getObjectAsBytes(objectRequest)
        return response
    }

    private fun createJsonMapper(): ObjectMapper {
        val mapper = ObjectMapper()
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        return mapper
    }
}

fun main(args : Array<String>) {
    val foo = Handler()
    val result = foo.downloadFile( "com-overstock-sns-test", "buyers_pick.csv" )
    val length = result.response().contentLength()
    println( "Length is $length")
}