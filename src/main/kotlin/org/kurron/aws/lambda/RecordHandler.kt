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
import software.amazon.awssdk.services.dynamodb.model.*
import java.lang.management.ManagementFactory
import java.net.URI
import java.util.*

/**
 * AWS Lambda entry point.
 */
class RecordHandler: RequestHandler<SQSEvent, Unit> {
    private val jsonMapper = createJsonMapper()

    override fun handleRequest(input: SQSEvent, context: Context) {
        dumpJvmSettings(context)


        // TODO: see if using streams instead of loops is more readable and/or efficient
        input.records.forEach { sqsRecord ->
            context.logger.log( "Processing ${sqsRecord.body}")
            val holder = jsonMapper.readValue<SkuProductRowHolder>(sqsRecord.body)
            holder.rows.forEach{ row ->
                // calculate an id for the row
                // see if it exists in DynamoDB
                // if not then import it into the system
                // add the row to DynamoDB
                val jsonForm = jsonMapper.writeValueAsString( row )
                val id = DigestUtils( "SHA-1" ).digestAsHex( jsonForm )
                val dynamoDB = DynamoDbClient.builder().build()
                val tableName = Optional.ofNullable(System.getenv("TABLE_NAME")).orElse( "TABLE NOT PROVIDED")
                val key = hashMapOf( "id" to AttributeValue.builder().s( id ).build() )
                val request = GetItemRequest.builder().tableName( tableName ).key( key ).build()
                val getResponse = dynamoDB.getItem( request )
                context.logger.log( "DynamoDB says ${getResponse.sdkHttpResponse().statusCode()}")
                val exists = getResponse.item()  != null
                context.logger.log( "DynamoDB says $id exists: $exists")
                if ( exists ) {
                    context.logger.log( "$id has previously been processed. Nothing to do.")
                }
                else {
                    // haven't seen it before
                    // update the system
                    // save the record as being seen
                }

                //val item = hashMapOf( "json" to AttributeValue.builder().s( jsonForm ).build() )
                //val putResponse = dynamoDB.putItem( PutItemRequest.builder().tableName( tableName ).item( item ).build() )

            }
        }
        context.logger.log( "Processing complete.")
    }

    private fun dumpJvmSettings(context: Context) {
        val runtimeMxBean = ManagementFactory.getRuntimeMXBean()
        val arguments = runtimeMxBean.inputArguments
        arguments.forEach {
            context.logger.log(it)
        }
    }

    fun createJsonMapper(): ObjectMapper {
        val mapper = ObjectMapper().registerModule(KotlinModule())
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        return mapper
    }
}

fun main(args : Array<String>) {
    println( "Hello, World! args is ${args.size} long.")
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
    val key = hashMapOf( "id" to AttributeValue.builder().s( id ).build() )

/*
    val request = GetItemRequest.builder().tableName( tableName ).key( key ).build()
    val getResponse = dynamoDB.getItem( request )
    println( "DynamoDB says ${getResponse.sdkHttpResponse().statusCode()}" )
    val exists = !getResponse.item().isEmpty()
    println( "DynamoDB says $id $exists")
*/

    val version = UUID.randomUUID().toString()
    val updateExpression = """
        SET version = :version, json = :json, progress = :progress
        ADD modified_by :modified_by
    """.trimIndent()
    val conditionExpression = """
        attribute_not_exists(id) OR (NOT progress IN (:initial_state, :completed_state))
    """.trimIndent()
    val values = mapOf<String,AttributeValue>( ":version" to AttributeValue.builder().s( version ).build(),
                                                                     ":json" to AttributeValue.builder().s( json ).build(),
                                                                     ":modified_by" to AttributeValue.builder().ss( version ).build(),
                                                                     ":initial_state" to AttributeValue.builder().s( "IN_PROGRESS" ).build(),
                                                                     ":completed_state" to AttributeValue.builder().s( "COMPLETED" ).build(),
                                                                     ":progress" to AttributeValue.builder().s( "IN_PROGRESS" ).build())
    val request = UpdateItemRequest.builder().tableName( tableName )
                                                             .key( key )
                                                             .updateExpression( updateExpression )
                                                             .expressionAttributeValues( values )
                                                             .conditionExpression( conditionExpression )
                                                             .returnValues( ReturnValue.ALL_NEW ).build()
    try {
        val response = dynamoDB.updateItem( request )
        println( "DynamoDB says ${response.sdkHttpResponse().statusCode()}" )
        response.attributes().orEmpty().entries.forEach { println( "${it.key} = ${it.value.s()}" ) }
        response.attributes()["modified_by"]?.ss()?.forEach{ println( "Modified by $it" ) }
    } catch (e: ConditionalCheckFailedException) {
        println( "Record already exists. Nothing to process.")
    }
}