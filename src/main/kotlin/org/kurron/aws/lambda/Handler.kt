package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SNSEvent
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvParser
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.PublishRequest
import software.amazon.awssdk.services.sns.model.PublishResponse
import java.io.InputStream
import java.lang.management.ManagementFactory
import java.util.*


/**
 * AWS Lambda entry point.
 */
class Handler: RequestHandler<SNSEvent,Unit> {
    override fun handleRequest(input: SNSEvent, context: Context) {
        dumpJvmSettings(context)

        val jsonMapper = createJsonMapper()
        val csvResources = createCsvMapper()
        val s3 = S3Client.builder().build()
        val sns = SnsClient.builder().build()

        // TODO: see if using streams instead of loops is more readable and/or efficient
        input.records.forEach { snsRecord ->
            dumpMessageAttributes(snsRecord, context)

            val event = toS3Event(snsRecord, context, jsonMapper)
            event.records.forEach { s3Record ->
                val stream = downloadFile(s3Record, context, s3)
                val rows = toRows(csvResources, stream)
                // TODO: the iterator will stop iterating if any record contains bad data, like a bad character code. Haven't found a way to change that behavior just yet.
                rows.forEach { row ->
                    publishRecord(jsonMapper, row, sns, context)
                }
            }
        }
    }

    private fun dumpJvmSettings(context: Context) {
        val runtimeMxBean = ManagementFactory.getRuntimeMXBean()
        val arguments = runtimeMxBean.inputArguments
        arguments.forEach {
            context.logger.log(it)
        }
    }

    private fun publishRecord(jsonMapper: ObjectMapper, row: SkuProductRow, sns: SnsClient, context: Context): PublishResponse {
        context.logger.log( "$row" )
        val message = jsonMapper.writeValueAsString(row)
        val topicArn: String = Optional.ofNullable(System.getenv("TOPIC_ARN")).orElseThrow { IllegalStateException("TOPIC_ARN was not provided!") }
        val request = PublishRequest.builder().topicArn(topicArn).message(message).build()
        val response = sns.publish(request)
        context.logger.log("SNS message id: ${response.messageId()}")
        return response
    }

    private fun toRows(resources: CsvResources, stream: InputStream): MappingIterator<SkuProductRow> {
        val reader = resources.mapper.readerFor(SkuProductRow::class.java).with(resources.schema)
        return reader.readValues(stream)
    }

    private fun toS3Event(snsRecord: SNSEvent.SNSRecord, context: Context, jsonMapper: ObjectMapper): S3Event {
        val json = snsRecord.sns.message
        context.logger.log("message = $json")
        return jsonMapper.readValue(json, object : TypeReference<S3Event>() {})
    }

    private fun dumpMessageAttributes(snsRecord: SNSEvent.SNSRecord, context: Context) {
        snsRecord.sns.messageAttributes.forEach { t, u ->
            context.logger.log("$t = $u")
        }
    }

    private fun downloadFile(s3Record: Record, context: Context, s3: S3Client): InputStream {
        val region = s3Record.region
        val bucket = s3Record.record.bucket.name
        val key = s3Record.record.data.key
        context.logger.log("Initiating download from S3: region = $region, bucket = $bucket, key = $key")

        val objectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build()
        val response = s3.getObject(objectRequest)
        context.logger.log("Just download $bucket/$key which was ${response.response().contentLength()} bytes long.")
        return response.buffered()
    }

    data class CsvResources(val mapper: CsvMapper, val schema: CsvSchema)

    private fun createCsvMapper(): CsvResources {
        val csvMapper = CsvMapper()
        csvMapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES)
        csvMapper.enable(CsvParser.Feature.TRIM_SPACES)
        val schema = CsvSchema.emptySchema().withHeader()
        return CsvResources( csvMapper, schema )
    }

    private fun createJsonMapper(): ObjectMapper {
        val mapper = ObjectMapper()
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        return mapper
    }
}

fun main(args : Array<String>) {
    println( "Hello, World! args is ${args.size} long.")
}