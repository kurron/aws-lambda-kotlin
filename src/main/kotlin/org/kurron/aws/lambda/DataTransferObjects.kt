package org.kurron.aws.lambda

import com.fasterxml.jackson.annotation.JsonProperty

class S3Event( @JsonProperty("Records") val records: List<Record> )

class Record( @JsonProperty("awsRegion") val region: String, @JsonProperty("s3") val record: S3 )

class S3( @JsonProperty("bucket") val bucket: Bucket, @JsonProperty("object") val data: Data  )

class Bucket( @JsonProperty("name") val name: String )

class Data( @JsonProperty("key") val key: String )