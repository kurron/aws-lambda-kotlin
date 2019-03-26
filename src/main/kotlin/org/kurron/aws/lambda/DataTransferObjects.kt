package org.kurron.aws.lambda

import com.fasterxml.jackson.annotation.JsonProperty

data class S3Event( @JsonProperty("Records") val records: List<Record> )

data class Record( @JsonProperty("awsRegion") val region: String, @JsonProperty("s3") val record: S3 )

data class S3( @JsonProperty("bucket") val bucket: Bucket, @JsonProperty("object") val data: Data  )

data class Bucket( @JsonProperty("name") val name: String )

data class Data( @JsonProperty("key") val key: String )

data class Row( @JsonProperty("sku") val sku: String, @JsonProperty("status") val status: String, @JsonProperty("force") val force: String)
