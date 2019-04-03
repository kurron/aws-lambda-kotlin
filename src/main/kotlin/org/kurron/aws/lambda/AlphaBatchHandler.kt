package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.fasterxml.jackson.module.kotlin.readValue

/**
 * Decomposes a batch of Alpha messages into individual messages.
 */
class AlphaBatchHandler: RequestHandler<SQSEvent, Unit> {
    private val mapper = createJsonMapper()
    private val sns = snsClient()
    private val topicArn = loadEnvironmentVariable( "TOPIC_ARN" )

    override fun handleRequest(input: SQSEvent, context: Context) {
        dumpJvmSettings(context)

        // TODO: see if using streams instead of loops is more readable and/or efficient
        input.records.forEach { sqsRecord ->
            context.logger.log( "Processing ${sqsRecord.body}")
            val routingKey = sqsRecord.messageAttributes.get("routing-key")?.stringValue ?: "routing key not provided"
            val holder = mapper.readValue<SkuProductRowHolder>(sqsRecord.body)
            holder.rows.forEach{ row ->
                val request = createPublishRequest(row, mapper, topicArn, routingKey)
                val response = sns.publish(request)
                assert( response.sdkHttpResponse().isSuccessful )
                context.logger.log( "${response.messageId()} event sent to SNS $topicArn")
            }
        }
        context.logger.log( "Processing complete.")
    }
}