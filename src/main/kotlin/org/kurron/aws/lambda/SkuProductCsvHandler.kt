package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SNSEvent
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvParser
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.PublishRequest
import java.io.InputStream
import java.lang.management.ManagementFactory
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


/**
 * AWS Lambda entry point.
 */
class SkuProductCsvHandler: RequestHandler<SNSEvent,Unit> {
    private val executor = Executors.newFixedThreadPool(64)
    private val jsonMapper = createJsonMapper()
    private val csvResources = createCsvMapper()
    private val s3 = S3Client.builder().build()
    private val sns = SnsClient.builder().build()
    private val capacity = 256
    private val maximumPayloadSize = 256_000

    override fun handleRequest(input: SNSEvent, context: Context) {
        dumpJvmSettings(context)


        // TODO: see if using streams instead of loops is more readable and/or efficient
        input.records.forEach { snsRecord ->
            dumpMessageAttributes(snsRecord, context)

            var counter = 0
            var totalLength = 0
            var batch = ArrayList<SkuProductRow>(capacity)

            val event = toS3Event(snsRecord, context, jsonMapper)
            event.records.forEach { s3Record ->
                val stream = downloadFile(s3Record, context, s3)
                val rows = toRows(csvResources, stream)
                // TODO: the iterator will stop iterating if any record contains bad data, like a bad character code. Haven't found a way to change that behavior just yet.
                rows.forEach { row ->
                    // the reason we are stuffing as much as we can into a single message is due to the fact you can easily exceed 15 minutes when publishing millions of messages
                    val rowLength = jsonMapper.writeValueAsString(row).length
                    val projectedLength = totalLength + rowLength

                    if ( projectedLength <= maximumPayloadSize) {
                        batch.add( row )
                    }
                    else {
                        executor.submit { publishMessage(jsonMapper, SkuProductRowHolder( batch ), context, sns) }

                        // reset for the next batch
                        totalLength = 0
                        batch = ArrayList(capacity)
                        batch.add( row )
                    }

                    totalLength += rowLength

                    if ( 0 == counter++ % 1000 ) {
                        context.logger.log( "We have processed $counter records")
                    }
                }
            }
        }

        executor.shutdown()
        context.logger.log( "Awaiting background jobs to complete....")
        executor.awaitTermination(15, TimeUnit.MINUTES)
        context.logger.log( "Background jobs completed." )
    }

    private fun publishMessage(mapper: ObjectMapper, data: SkuProductRowHolder, context: Context, sns: SnsClient) {
        val message = mapper.writeValueAsString(data)
        context.logger.log("Submitting a message with : ${message.length} characters")
        val topicArn: String = Optional.ofNullable(System.getenv("TOPIC_ARN")).orElseThrow { IllegalStateException("TOPIC_ARN was not provided!") }
        val request = PublishRequest.builder().topicArn(topicArn).message(message).build()
        try {
            val response = sns.publish(request)
            assert( response.sdkHttpResponse().isSuccessful )
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

    private fun toRows(resources: CsvResources, stream: InputStream): MappingIterator<SkuProductRow> {
        val reader = resources.mapper.readerFor(SkuProductRow::class.java).with(resources.schema)
        return reader.readValues(stream)
    }

    private fun toS3Event(snsRecord: SNSEvent.SNSRecord, context: Context, jsonMapper: ObjectMapper): S3Message {
        val json = snsRecord.sns.message
        context.logger.log("message = $json")
        return jsonMapper.readValue(json)
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
        return response.buffered( 512 * 1024 )
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
        val mapper = ObjectMapper().registerModule(KotlinModule())
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        return mapper
    }
}

fun main(args : Array<String>) {
    println( "Hello, World! args is ${args.size} long.")
}