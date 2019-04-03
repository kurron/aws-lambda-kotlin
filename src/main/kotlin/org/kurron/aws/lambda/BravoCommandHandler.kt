package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import org.apache.commons.codec.digest.DigestUtils

/**
 * Processes command messages by integrating with an external system and tracking the processing in DynamoDB.
 */
class BravoCommandHandler: RequestHandler<SQSEvent, Unit> {
    private val dynamoDB = dynamoClient()
    private val tableName = loadEnvironmentVariable( "TABLE_NAME" )

    override fun handleRequest(input: SQSEvent, context: Context) {
        dumpJvmSettings(context)

        // TODO: see if using streams instead of loops is more readable and/or efficient
        input.records.forEach { sqsRecord ->
            val json = sqsRecord.body
            context.logger.log( "Processing $json")
            val id = calculateDigest(json)
            context.logger.log( "Processing record $id from $sqsRecord.body")

            val version = loadRecord( context, dynamoDB, json, tableName, id )
            if ( version.isNotEmpty() ) {
                doWork( context )
                updateProgress( context, dynamoDB, tableName, id, version )
            }
        }
        context.logger.log( "Processing complete.")
    }
}