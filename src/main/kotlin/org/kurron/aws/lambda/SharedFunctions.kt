package org.kurron.aws.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.SNSEvent
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvParser
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.commons.codec.digest.DigestUtils
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.MessageAttributeValue
import software.amazon.awssdk.services.sns.model.PublishRequest
import java.io.InputStream
import java.lang.management.ManagementFactory
import java.util.*

fun createJsonMapper(): ObjectMapper {
    val mapper = ObjectMapper().registerModule(KotlinModule())
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    return mapper
}

fun dumpJvmSettings(context: Context) {
    val runtimeMxBean = ManagementFactory.getRuntimeMXBean()
    val arguments = runtimeMxBean.inputArguments
    arguments.forEach {
        context.logger.log(it)
    }
}

fun loadEnvironmentVariable( key: String ) : String {
    return Optional.ofNullable(System.getenv(key)).orElseThrow { IllegalStateException("$key was not provided!") }
}

fun snsClient() : SnsClient {
    return SnsClient.builder().build()
}

fun dynamoClient(): DynamoDbClient {
    return DynamoDbClient.builder().build()
}

fun s3Client(): S3Client {
    return S3Client.builder().build()
}

fun calculateDigest( json: String ): String {
    return DigestUtils("SHA-1").digestAsHex(json)
}

fun createPublishRequest(row: SkuProductRow, mapper: ObjectMapper, topicArn: String, routingKey: String): PublishRequest {
    val message = mapper.writeValueAsString(row)
    val value = MessageAttributeValue.builder().dataType("String").stringValue(routingKey).build()
    return PublishRequest.builder()
            .topicArn(topicArn)
            .message(message)
            .messageAttributes(hashMapOf("routing-key" to value))
            .build()
}

fun createPublishRequest(row: BuyersPickRow, mapper: ObjectMapper, topicArn: String, routingKey: String): PublishRequest {
    val message = mapper.writeValueAsString(row)
    val value = MessageAttributeValue.builder().dataType("String").stringValue(routingKey).build()
    return PublishRequest.builder()
            .topicArn(topicArn)
            .message(message)
            .messageAttributes(hashMapOf("routing-key" to value))
            .build()
}

fun doWork(context: Context) {
    // pretend to do some work
    context.logger.log( "Pretending to do some work." )
    Thread.sleep( 1000 )
}

fun updateProgress(context: Context, dynamoDB: DynamoDbClient, tableName: String, id: String, version: String) {
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

fun loadRecord(context: Context, dynamoDB: DynamoDbClient, rowAsJson: String, tableName: String, id: String): String {
    val request = generateUpsertRequest(rowAsJson, tableName, id)
    return try {
        val response = dynamoDB.updateItem(request)
        // TODO: honestly, if version is missing then something is very, very wrong
        val version = response.attributes()["version"]?.s().orEmpty()
        context.logger.log("Loaded record $id, returning version $version")
        version
    } catch (e: ConditionalCheckFailedException) {
        context.logger.log("Record $id already exists. Nothing to process.")
        ""
    }
}

/**
 * This function either creates a new record or migrates its progress field to IN_PROGRESS.
 * @throws ConditionalCheckFailedException if an existing record is found but is not in the proper state, probably indicating that someone else has already started processing.
 */
private fun generateUpsertRequest(json: String, tableName: String, id: String): UpdateItemRequest {
    val requestID = UUID.randomUUID().toString()
    val key = hashMapOf("id" to AttributeValue.builder().s(id).build())
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

data class CsvResources(val mapper: CsvMapper, val schema: CsvSchema)

fun createCsvMapper(): CsvResources {
    val csvMapper = CsvMapper()
    csvMapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES)
    csvMapper.enable(CsvParser.Feature.TRIM_SPACES)
    val schema = CsvSchema.emptySchema().withHeader()
    return CsvResources( csvMapper, schema )
}

fun downloadFile(event: S3ChangeEvent, context: Context, s3: S3Client): InputStream {
    val region = event.region
    val bucket = event.bucket
    val key = event.key
    context.logger.log("Initiating download from S3: region = $region, bucket = $bucket, key = $key")

    val objectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build()
    val response = s3.getObject(objectRequest)
    context.logger.log("Just download $bucket/$key which was ${response.response().contentLength()} bytes long.")
    return response.buffered( 512 * 1024 )
}

fun toChangeEvent(record: SNSEvent.SNSRecord, context: Context, mapper: ObjectMapper): S3ChangeEvent {
    val event = mapper.readValue<S3ChangeEvent>(record.sns.message)
    context.logger.log("Just heard $event")
    return event
}

