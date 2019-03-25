package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SNSEvent

/**
 * AWS Lambda entry point.
 */
class Handler: RequestHandler<SNSEvent,Unit> {
    override fun handleRequest(input: SNSEvent, context: Context) {
        input.records.forEach {
            context.logger.log("message = $it.sns.message")
        }
    }
}