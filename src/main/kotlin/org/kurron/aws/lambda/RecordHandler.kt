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
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import java.lang.management.ManagementFactory
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
                val getResponse = dynamoDB.getItem( GetItemRequest.builder().tableName( tableName ).key( hashMapOf( "id" to AttributeValue.builder().s( id ).build() ) ).build() )
                context.logger.log( "DynamoDB says ${getResponse.sdkHttpResponse().statusCode()}")

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

    private fun createJsonMapper(): ObjectMapper {
        val mapper = ObjectMapper().registerModule(KotlinModule())
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        return mapper
    }
}