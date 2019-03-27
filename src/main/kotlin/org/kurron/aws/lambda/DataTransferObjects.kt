package org.kurron.aws.lambda

import com.fasterxml.jackson.annotation.JsonProperty

data class S3Event( @JsonProperty("Records") val records: List<Record> )

data class Record( @JsonProperty("awsRegion") val region: String, @JsonProperty("s3") val record: S3 )

data class S3( @JsonProperty("bucket") val bucket: Bucket, @JsonProperty("object") val data: Data  )

data class Bucket( @JsonProperty("name") val name: String )

data class Data( @JsonProperty("key") val key: String )

data class Row( @JsonProperty("sku") val sku: String, @JsonProperty("status") val status: String, @JsonProperty("force") val force: String)


// "sku_long","sku_short","product_id","option_id","sub_category_id","sub_cat","dept_id","dept","catg_id","store_id","store","category","catid","color","style","image_url","product_url","variant_url"
data class SkuProductRow( @JsonProperty("sku_long") val skuLong: String,
                          @JsonProperty("sku_short") val skuShort: String,
                          @JsonProperty("product_id") val productID: String,
                          @JsonProperty("option_id") val optionID: String,
                          @JsonProperty("sub_category_id") val subCategoryID: String,
                          @JsonProperty("sub_cat") val subCategory: String,
                          @JsonProperty("dept_id") val departmentID: String,
                          @JsonProperty("dept") val department: String,
                          @JsonProperty("catg_id") val catalogID: String,
                          @JsonProperty("store_id") val storeID: String,
                          @JsonProperty("store") val store: String,
                          @JsonProperty("category") val category: String,
                          @JsonProperty("catid") val categoryID: String,
                          @JsonProperty("color") val color: String,
                          @JsonProperty("style") val style: String,
                          @JsonProperty("image_url") val imageURL: String,
                          @JsonProperty("product_url") val productURL: String,
                          @JsonProperty("variant_url") val variantURL: String)
