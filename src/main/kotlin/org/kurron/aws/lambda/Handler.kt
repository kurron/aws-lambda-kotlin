package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler

/**
 * AWS Lambda entry point.
 */
class Handler: RequestHandler<String,String> {
    override fun handleRequest(input: String, context: Context): String {
        context.logger.log("Input = $input")
        return "Hello $input from Kotlin"
    }
}