package org.kurron.aws.lambda

import com.fasterxml.jackson.annotation.JsonProperty

data class SkuProductRow( @JsonProperty("sku_long")        val skuLong: String,
                          @JsonProperty("sku_short")       val skuShort: String,
                          @JsonProperty("product_id")      val productID: String,
                          @JsonProperty("option_id")       val optionID: String,
                          @JsonProperty("sub_category_id") val subCategoryID: String,
                          @JsonProperty("sub_cat")         val subCategory: String,
                          @JsonProperty("dept_id")         val departmentID: String,
                          @JsonProperty("dept")            val department: String,
                          @JsonProperty("catg_id")         val catalogID: String,
                          @JsonProperty("store_id")        val storeID: String,
                          @JsonProperty("store")           val store: String,
                          @JsonProperty("category")        val category: String,
                          @JsonProperty("catid")           val categoryID: String,
                          @JsonProperty("color")           val color: String,
                          @JsonProperty("style")           val style: String,
                          @JsonProperty("image_url")       val imageURL: String,
                          @JsonProperty("product_url")     val productURL: String,
                          @JsonProperty("variant_url")     val variantURL: String)

data class SkuProductRowHolder( @JsonProperty("rows") val rows: List<SkuProductRow>)

data class S3ChangeEvent( @JsonProperty("region") val region: String,
                          @JsonProperty("bucket") val bucket: String,
                          @JsonProperty("key")    val key: String)

data class BuyersPickRow( @JsonProperty("sku")    val sku: String,
                          @JsonProperty("status") val status: String,
                          @JsonProperty("force")  val force: String)

data class BuyersPickRowHolder( @JsonProperty("rows") val rows: List<BuyersPickRow> )
