package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.commons.codec.digest.DigestUtils
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.lang.management.ManagementFactory
import java.util.*

/**
 * Processes command messages by integrating with an external system and tracking the processing in DynamoDB.
 */
class BravoCommandHandler: RequestHandler<SQSEvent, Unit> {
    private val jsonMapper = createJsonMapper()
    private val dynamoDB = DynamoDbClient.builder().build()
    private val tableName = Optional.ofNullable(System.getenv("TABLE_NAME")).orElse( "TABLE NOT PROVIDED")

    override fun handleRequest(input: SQSEvent, context: Context) {
        dumpJvmSettings(context)


        // TODO: see if using streams instead of loops is more readable and/or efficient
        input.records.forEach { sqsRecord ->
            context.logger.log( "Processing ${sqsRecord.body}")
            val holder = jsonMapper.readValue<BuyersPickRowHolder>(sqsRecord.body)

            holder.rows.forEach{ row ->
                val rowAsJson = jsonMapper.writeValueAsString( row )
                val id = DigestUtils("SHA-1").digestAsHex( rowAsJson )
                context.logger.log( "Processing record $id from $rowAsJson")

                val version = loadRecord( context, dynamoDB, rowAsJson, tableName, id )
                if ( version.isNotEmpty() ) {
                    doWork( context )
                    updateProgress( context, dynamoDB, tableName, id, version )
                }
            }
        }
        context.logger.log( "Processing complete.")
    }

    private fun doWork(context: Context) {
        // pretend to do some work
        context.logger.log( "Pretending to do some work." )
        Thread.sleep( 1000 )
    }

    private fun updateProgress(context: Context, dynamoDB: DynamoDbClient, tableName: String, id: String, version: String) {
        val request = generateStatusChangeRequest( version, tableName, id )
        try {
            val response = dynamoDB.updateItem( request )
            val progress = response.attributes()["progress"]?.s()
            context.logger.log( "Record $id has been updated to a status of $progress")
        }
        catch (e: ConditionalCheckFailedException) {
            // TODO: in production we might want to reread the record and decide whether to retry or not
            context.logger.log( "Optimistic lock failure.  Record $id not updated.")
        }
    }

    private fun loadRecord(context: Context, dynamoDB: DynamoDbClient, rowAsJson: String, tableName: String, id: String ) : String {
        val request = generateUpsertRequest(rowAsJson, tableName, id)
        return try {
            val response = dynamoDB.updateItem( request )
            // TODO: honestly, if version is missing then something is very, very wrong
            val version = response.attributes()["version"]?.s().orEmpty()
            context.logger.log( "Loaded record $id, returning version $version")
            version
        }
        catch ( e: ConditionalCheckFailedException) {
            context.logger.log( "Record $id already exists. Nothing to process.")
            ""
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

    /**
     * This function either creates a new record or migrates its progress field to IN_PROGRESS.
     * @throws ConditionalCheckFailedException if an existing record is found but is not in the proper state, probably indicating that someone else has already started processing.
     */
    private fun generateUpsertRequest(json: String, tableName: String, id: String ): UpdateItemRequest {
        val requestID = UUID.randomUUID().toString()
        val key = hashMapOf( "id" to AttributeValue.builder().s( id ).build() )
        val version = UUID.randomUUID().toString()
        val updateExpression = "SET version = :version, json = :json, progress = :progress ADD modified_by :modified_by"
        // NOTE: found that you have substitute the values in the list or the expression doesn't properly get evaluated -- using literals does NOT work!
        val conditionExpression = "attribute_not_exists(id) OR (NOT progress IN (:initial_state, :completed_state))"
        val values = mapOf<String, AttributeValue>(":version" to AttributeValue.builder().s(version).build(),
                                                                         ":json" to AttributeValue.builder().s(json).build(),
                                                                         ":modified_by" to AttributeValue.builder().ss(requestID).build(),
                                                                         ":initial_state" to AttributeValue.builder().s("IN_PROGRESS").build(),
                                                                         ":completed_state" to AttributeValue.builder().s("COMPLETED").build(),
                                                                         ":progress" to AttributeValue.builder().s("IN_PROGRESS").build())
        return UpdateItemRequest.builder().tableName(tableName)
                                          .key(key)
                                          .updateExpression(updateExpression)
                                          .expressionAttributeValues(values)
                                          .conditionExpression(conditionExpression)
                                          .returnValues(ReturnValue.ALL_NEW)
                                          .build()
    }

    /**
     * This function migrates the record's progress field to a value of COMPLETED but only if the optimistic lock allows.
     * @throws ConditionalCheckFailedException if the optimistic lock fails.
     */
    private fun generateStatusChangeRequest(version: String, tableName: String, id: String ): UpdateItemRequest {
        val requestID = UUID.randomUUID().toString()
        val key = hashMapOf( "id" to AttributeValue.builder().s( id ).build() )
        val updateExpression = "SET progress = :progress ADD modified_by :modified_by"
        val conditionExpression = "version = :version"
        val values = mapOf<String, AttributeValue>(":version" to AttributeValue.builder().s(version).build(),
                                                                         ":modified_by" to AttributeValue.builder().ss(requestID).build(),
                                                                         ":progress" to AttributeValue.builder().s("COMPLETED").build())
        return UpdateItemRequest.builder().tableName(tableName)
                                          .key(key)
                                          .updateExpression(updateExpression)
                                          .expressionAttributeValues(values)
                                          .conditionExpression(conditionExpression)
                                          .returnValues(ReturnValue.UPDATED_NEW).build()
    }
}