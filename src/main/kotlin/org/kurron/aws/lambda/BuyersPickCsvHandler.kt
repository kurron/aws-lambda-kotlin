package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SNSEvent
import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.databind.ObjectMapper
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.MessageAttributeValue
import software.amazon.awssdk.services.sns.model.PublishRequest
import java.io.InputStream
import java.util.*

/**
 * Parses the CSV file written in the bravo format, transforming the records into JSON messages published to an SNS Topic.
 */
class BuyersPickCsvHandler: RequestHandler<SNSEvent, Unit> {
    private val topicArn = loadEnvironmentVariable( "TOPIC_ARN" )
    private val jsonMapper = createJsonMapper()
    private val csvResources = createCsvMapper()
    private val s3 = s3Client()
    private val sns = snsClient()
    private val capacity = 256
    private val maximumPayloadSize = 256_000

    override fun handleRequest(input: SNSEvent, context: Context) {
        dumpJvmSettings(context)

        input.records.forEach { record ->
            val event = toChangeEvent(record, context, jsonMapper)

            var counter = 0
            var totalLength = 0
            var batch = ArrayList<BuyersPickRow>(capacity)

            val stream = downloadFile(event, context, s3)
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
                    publishMessage(jsonMapper, BuyersPickRowHolder(batch), context, sns, event.key)

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
            // in case the records are so small we never hit the maximum size and trigger a publish
            if ( batch.isNotEmpty() ) {
                publishMessage(jsonMapper, BuyersPickRowHolder(batch), context, sns, event.key)
            }
            context.logger.log( "Processing completed.")
        }
    }

    private fun publishMessage(mapper: ObjectMapper, data: BuyersPickRowHolder, context: Context, sns: SnsClient, routingKey: String) {
        val value = MessageAttributeValue.builder().dataType("String").stringValue(routingKey).build()
        val message = mapper.writeValueAsString(data)
        context.logger.log("Submitting a message with : ${message.length} characters")
        val request = PublishRequest.builder()
                                                 .topicArn(topicArn)
                                                 .message(message)
                                                 .messageAttributes(hashMapOf("routing-key" to value))
                                                 .build()
        try {
            val response = sns.publish(request)
            assert( response.sdkHttpResponse().isSuccessful )
        } catch (e: Exception) {
            context.logger.log( "Unable to publish message: ${e.message}" )
        }
    }

    private fun toRows(resources: CsvResources, stream: InputStream): MappingIterator<BuyersPickRow> {
        val reader = resources.mapper.readerFor(BuyersPickRow::class.java).with(resources.schema)
        return reader.readValues(stream)
    }

}