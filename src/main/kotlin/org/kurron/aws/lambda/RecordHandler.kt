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
 * AWS Lambda entry point.
 */
class RecordHandler: RequestHandler<SQSEvent, Unit> {
    private val jsonMapper = createJsonMapper()

    override fun handleRequest(input: SQSEvent, context: Context) {
        dumpJvmSettings(context)

        val dynamoDB = DynamoDbClient.builder().build()
        val tableName = Optional.ofNullable(System.getenv("TABLE_NAME")).orElse( "TABLE NOT PROVIDED")

        // TODO: see if using streams instead of loops is more readable and/or efficient
        input.records.forEach { sqsRecord ->
            context.logger.log( "Processing ${sqsRecord.body}")
            val holder = jsonMapper.readValue<SkuProductRowHolder>(sqsRecord.body)

            holder.rows.forEach{ row ->
                val rowAsJson = jsonMapper.writeValueAsString( row )
                val id = DigestUtils( "SHA-1" ).digestAsHex( rowAsJson )
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

    private fun loadRecord( context: Context, dynamoDB: DynamoDbClient, rowAsJson: String, tableName: String, id: String ) : String {
        val request = generateUpsertRequest(rowAsJson, tableName, id)
        return try {
            val response = dynamoDB.updateItem( request )
            // TODO: honestly, if version is missing then something is very wrong
            val version = response.attributes()["version"]?.s().orEmpty()
            context.logger.log( "Loaded record $id, returning version $version")
            version
        }
        catch ( e: ConditionalCheckFailedException ) {
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

fun main(args : Array<String>) {
    println( "Hello, World! args is ${args.size} long.")
/*

    var handler = RecordHandler()
    var mapper = handler.createJsonMapper()
    var from = SkuProductRow( skuLong = "alpha",
            skuShort = "bravo",
            productID = "charlie",
            optionID = "delta",
            subCategoryID = "echo",
            subCategory = "foxtrot",
            departmentID = "gulf",
            department = "hotel",
            catalogID = "indigo",
            storeID = "juliette",
            store = "kilo",
            category = "lima",
            categoryID = "mike",
            color = "november",
            style = "oscar",
            imageURL = "papa",
            productURL = "quebec",
            variantURL = "romeo")
    val json = mapper.writeValueAsString( from )
    val id = DigestUtils( "SHA-1" ).digestAsHex( json )
    val dynamoDB = DynamoDbClient.builder().endpointOverride( URI( "http://localhost:8000" ) ).build()
    val tableName = Optional.ofNullable(System.getenv("TABLE_NAME")).orElse( "TABLE NOT PROVIDED")

    val upsertRequest = generateUpsertRequest(json, tableName, id)
    try {
        val upsertResponse = dynamoDB.updateItem( upsertRequest )
        println( "DynamoDB says ${upsertResponse.sdkHttpResponse().statusCode()}" )
        upsertResponse.attributes().orEmpty().entries.forEach { println( "${it.key} = ${it.value.s()}" ) }
        upsertResponse.attributes()["modified_by"]?.ss()?.forEach{ println( "Modified by $it" ) }
        val version = upsertResponse.attributes()["version"]?.s()

        // pretend to do some work
        Thread.sleep( 1000 * 1)

        val updateRequest = generateStatusChangeRequest( version!!, tableName, id )
        val updateResponse = dynamoDB.updateItem( updateRequest )
        println( "DynamoDB says ${updateResponse.sdkHttpResponse().statusCode()}" )
        updateResponse.attributes().orEmpty().entries.forEach { println( "${it.key} = ${it.value.s()}" ) }
        updateResponse.attributes()["modified_by"]?.ss()?.forEach{ println( "Modified by $it" ) }

    } catch (e: ConditionalCheckFailedException) {
        println( "Record already exists. Nothing to process.")
    }
*/
}



