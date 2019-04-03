package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSEvent

/**
 * Processes command messages by integrating with an external system and tracking the processing in DynamoDB.
 */
class AlphaCommandHandler: RequestHandler<SQSEvent, Unit> {
    private val dynamoDB = dynamoClient()
    private val tableName = loadEnvironmentVariable("TABLE_NAME")

    override fun handleRequest(input: SQSEvent, context: Context) {
        dumpJvmSettings(context)

        // TODO: see if using streams instead of loops is more readable and/or efficient
        input.records.forEach { sqsRecord ->
            val json = sqsRecord.body
            context.logger.log("Processing $json")
            val id = calculateDigest(json)
            context.logger.log("Processing record $id from $sqsRecord.body")

            val version = loadRecord(context, dynamoDB, json, tableName, id)
            if (version.isNotEmpty()) {
                doWork(context)
                updateProgress(context, dynamoDB, tableName, id, version)
            }
        }
        context.logger.log("Processing complete.")
    }
}

fun main(args : Array<String>) {
    println( "Hello, World! args is ${args.size} long.")
/*

    var handler = AlphaCommandHandler()
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



