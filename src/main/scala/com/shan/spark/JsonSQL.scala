import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object sqlspark {
  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val df = spark.read.format("json").json("src/main/resources/companies.json")
    df.printSchema()

 /*   root
    |-- _id: struct (nullable = true)
    |    |-- $oid: string (nullable = true)
    |-- acquisition: struct (nullable = true)
    |    |-- acquired_day: long (nullable = true)
    |    |-- acquired_month: long (nullable = true)
    |    |-- acquired_year: long (nullable = true)
    |    |-- acquiring_company: struct (nullable = true)
    |    |    |-- name: string (nullable = true)
    |    |    |-- permalink: string (nullable = true)
    |    |-- price_amount: long (nullable = true)
    |    |-- price_currency_code: string (nullable = true)
    |    |-- source_description: string (nullable = true)
    |    |-- source_url: string (nullable = true)
    |    |-- term_code: string (nullable = true)
    |-- acquisitions: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- acquired_day: long (nullable = true)
    |    |    |-- acquired_month: long (nullable = true)
    |    |    |-- acquired_year: long (nullable = true)
    |    |    |-- company: struct (nullable = true)
    |    |    |    |-- name: string (nullable = true)
    |    |    |    |-- permalink: string (nullable = true)
    |    |    |-- price_amount: double (nullable = true)
    |    |    |-- price_currency_code: string (nullable = true)
    |    |    |-- source_description: string (nullable = true)
    |    |    |-- source_url: string (nullable = true)
    |    |    |-- term_code: string (nullable = true)
    |-- alias_list: string (nullable = true)
    |-- blog_feed_url: string (nullable = true)
    |-- blog_url: string (nullable = true)
    |-- category_code: string (nullable = true)
    |-- competitions: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- competitor: struct (nullable = true)
    |    |    |    |-- name: string (nullable = true)
    |    |    |    |-- permalink: string (nullable = true)
    |-- created_at: string (nullable = true)
    |-- crunchbase_url: string (nullable = true)
    |-- deadpooled_day: long (nullable = true)
    |-- deadpooled_month: long (nullable = true)
    |-- deadpooled_url: string (nullable = true)
    |-- deadpooled_year: long (nullable = true)
    |-- description: string (nullable = true)
    |-- email_address: string (nullable = true)
    |-- external_links: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- external_url: string (nullable = true)
    |    |    |-- title: string (nullable = true)
    |-- founded_day: long (nullable = true)
    |-- founded_month: long (nullable = true)
    |-- founded_year: long (nullable = true)
    |-- funding_rounds: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- funded_day: long (nullable = true)
    |    |    |-- funded_month: long (nullable = true)
    |    |    |-- funded_year: long (nullable = true)
    |    |    |-- id: long (nullable = true)
    |    |    |-- investments: array (nullable = true)
    |    |    |    |-- element: struct (containsNull = true)
    |    |    |    |    |-- company: struct (nullable = true)
    |    |    |    |    |    |-- name: string (nullable = true)
    |    |    |    |    |    |-- permalink: string (nullable = true)
    |    |    |    |    |-- financial_org: struct (nullable = true)
    |    |    |    |    |    |-- name: string (nullable = true)
    |    |    |    |    |    |-- permalink: string (nullable = true)
    |    |    |    |    |-- person: struct (nullable = true)
    |    |    |    |    |    |-- first_name: string (nullable = true)
    |    |    |    |    |    |-- last_name: string (nullable = true)
    |    |    |    |    |    |-- permalink: string (nullable = true)
    |    |    |-- raised_amount: double (nullable = true)
    |    |    |-- raised_currency_code: string (nullable = true)
    |    |    |-- round_code: string (nullable = true)
    |    |    |-- source_description: string (nullable = true)
    |    |    |-- source_url: string (nullable = true)
    |-- homepage_url: string (nullable = true)
    |-- image: struct (nullable = true)
    |    |-- attribution: string (nullable = true)
    |    |-- available_sizes: array (nullable = true)
    |    |    |-- element: array (containsNull = true)
    |    |    |    |-- element: string (containsNull = true)
    |-- investments: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- funding_round: struct (nullable = true)
    |    |    |    |-- company: struct (nullable = true)
    |    |    |    |    |-- name: string (nullable = true)
    |    |    |    |    |-- permalink: string (nullable = true)
    |    |    |    |-- funded_day: long (nullable = true)
    |    |    |    |-- funded_month: long (nullable = true)
    |    |    |    |-- funded_year: long (nullable = true)
    |    |    |    |-- raised_amount: long (nullable = true)
    |    |    |    |-- raised_currency_code: string (nullable = true)
    |    |    |    |-- round_code: string (nullable = true)
    |    |    |    |-- source_description: string (nullable = true)
    |    |    |    |-- source_url: string (nullable = true)
    |-- ipo: struct (nullable = true)
    |    |-- pub_day: long (nullable = true)
    |    |-- pub_month: long (nullable = true)
    |    |-- pub_year: long (nullable = true)
    |    |-- stock_symbol: string (nullable = true)
    |    |-- valuation_amount: long (nullable = true)
    |    |-- valuation_currency_code: string (nullable = true)
    |-- milestones: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- description: string (nullable = true)
    |    |    |-- id: long (nullable = true)
    |    |    |-- source_description: string (nullable = true)
    |    |    |-- source_text: string (nullable = true)
    |    |    |-- source_url: string (nullable = true)
    |    |    |-- stoneable: struct (nullable = true)
    |    |    |    |-- name: string (nullable = true)
    |    |    |    |-- permalink: string (nullable = true)
    |    |    |-- stoneable_type: string (nullable = true)
    |    |    |-- stoned_acquirer: string (nullable = true)
    |    |    |-- stoned_day: long (nullable = true)
    |    |    |-- stoned_month: long (nullable = true)
    |    |    |-- stoned_value: string (nullable = true)
    |    |    |-- stoned_value_type: string (nullable = true)
    |    |    |-- stoned_year: long (nullable = true)
    |-- name: string (nullable = true)
    |-- number_of_employees: long (nullable = true)
    |-- offices: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- address1: string (nullable = true)
    |    |    |-- address2: string (nullable = true)
    |    |    |-- city: string (nullable = true)
    |    |    |-- country_code: string (nullable = true)
    |    |    |-- description: string (nullable = true)
    |    |    |-- latitude: double (nullable = true)
    |    |    |-- longitude: double (nullable = true)
    |    |    |-- state_code: string (nullable = true)
    |    |    |-- zip_code: string (nullable = true)
    |-- overview: string (nullable = true)
    |-- partners: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- homepage_url: string (nullable = true)
    |    |    |-- link_1_name: string (nullable = true)
    |    |    |-- link_1_url: string (nullable = true)
    |    |    |-- link_2_name: string (nullable = true)
    |    |    |-- link_2_url: string (nullable = true)
    |    |    |-- link_3_name: string (nullable = true)
    |    |    |-- link_3_url: string (nullable = true)
    |    |    |-- partner_name: string (nullable = true)
    |-- permalink: string (nullable = true)
    |-- phone_number: string (nullable = true)
    |-- products: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- name: string (nullable = true)
    |    |    |-- permalink: string (nullable = true)
    |-- providerships: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- is_past: boolean (nullable = true)
    |    |    |-- provider: struct (nullable = true)
    |    |    |    |-- name: string (nullable = true)
    |    |    |    |-- permalink: string (nullable = true)
    |    |    |-- title: string (nullable = true)
    |-- relationships: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- is_past: boolean (nullable = true)
    |    |    |-- person: struct (nullable = true)
    |    |    |    |-- first_name: string (nullable = true)
    |    |    |    |-- last_name: string (nullable = true)
    |    |    |    |-- permalink: string (nullable = true)
    |    |    |-- title: string (nullable = true)
    |-- screenshots: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- attribution: string (nullable = true)
    |    |    |-- available_sizes: array (nullable = true)
    |    |    |    |-- element: array (containsNull = true)
    |    |    |    |    |-- element: string (containsNull = true)
    |-- tag_list: string (nullable = true)
    |-- total_money_raised: string (nullable = true)
    |-- twitter_username: string (nullable = true)
    |-- updated_at: string (nullable = true)
    |-- video_embeds: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- description: string (nullable = true)
    |    |    |-- embed_code: string (nullable = true)*/
    // Creates a temporary view using the DataFrame
    df.createOrReplaceTempView("companies")

    // SQL statements can be run by using the sql methods provided by spark
    val compNameDF = spark.sql("SELECT name,founded_year FROM companies WHERE number_of_employees  BETWEEN 47 AND 54")
    df.groupBy("founded_year").max("number_of_employees").show
/*  Output:

+------------+------------------------+
|founded_year|max(number_of_employees)|
+------------+------------------------+
|        1950|                   11000|
|        1840|                      35|
|        1919|                    null|
|        1936|                     600|
|        1951|                    null|
|        1958|                     100|
|        1921|                  107000|
|        1983|                    8000|
|        1972|                    6000|
|        1979|                   18200|
|        2007|                     900|
|        1887|                   35000|
|        1988|                   74000|
|        1899|                    null|
|        1986|                     715|
|        1949|                    null|
|        1846|                    3700|
|        1930|                   30175|
|        1969|                  221726|
|        1964|                   34300|
+------------+------------------------+
only showing top 20 rows
 */
    // DataFrame Query: Multiple filter chaining
    df.filter("name like 's%'")
      .filter("founded_year==2005")
      .show(10)
 /* Output:
   +--------------------+-----------+------------+----------+-------------+--------------------+-------------+------------+--------------------+--------------------+--------------+----------------+--------------+---------------+--------------------+--------------------+--------------------+-----------+-------------+------------+--------------------+--------------------+--------------------+-----------+----+----------+------------+-------------------+--------------------+--------------------+--------+------------+------------+--------------------+--------------------+--------------------+--------------------+--------------------+------------------+----------------+--------------------+------------+
|                 _id|acquisition|acquisitions|alias_list|blog_feed_url|            blog_url|category_code|competitions|          created_at|      crunchbase_url|deadpooled_day|deadpooled_month|deadpooled_url|deadpooled_year|         description|       email_address|      external_links|founded_day|founded_month|founded_year|      funding_rounds|        homepage_url|               image|investments| ipo|milestones|        name|number_of_employees|             offices|            overview|partners|   permalink|phone_number|            products|       providerships|       relationships|         screenshots|            tag_list|total_money_raised|twitter_username|          updated_at|video_embeds|
+--------------------+-----------+------------+----------+-------------+--------------------+-------------+------------+--------------------+--------------------+--------------+----------------+--------------+---------------+--------------------+--------------------+--------------------+-----------+-------------+------------+--------------------+--------------------+--------------------+-----------+----+----------+------------+-------------------+--------------------+--------------------+--------+------------+------------+--------------------+--------------------+--------------------+--------------------+--------------------+------------------+----------------+--------------------+------------+
|[52cdef7d4bab8bd6...|       null|          []|System One|             |                    |     software|          []|Wed Oct 22 22:37:...|http://www.crunch...|          null|            null|          null|           null|                    |office@semanticla...|[[http://www.tech...|          5|            2|        2005|[[5,2,2005,3702,W...|http://www.system...|[null,WrappedArra...|         []|null|        []|semanticlabs|                 23|[[Zollergasse 9-1...|<p>System One, ba...|      []|semanticlabs|+43507051110|[[System One Coll...|[[false,[TheMerge...|[[false,[Michael,...|                  []|semantic-web, col...|             €1.6M|                |Tue Oct 22 09:02:...|          []|
|[52cdef7e4bab8bd6...|       null|          []|      null|             |                    |     software|          []|Tue Dec 09 10:43:...|http://www.crunch...|          null|            null|          null|           null|                null|support@speroware...|                  []|          1|            1|        2005|                  []|http://www.sperow...|[null,WrappedArra...|         []|null|        []|   speroware|                  5|[[13th Main 1st C...|<p>Speroware is a...|      []|   speroware|            |                  []|                  []|[[false,[Ramu,Che...|[[null,WrappedArr...|                    |                $0|            null|Mon Nov 29 18:43:...|          []|
|[52cdef7e4bab8bd6...|       null|          []|          |             |http://blog.scanr...|       mobile|          []|Fri Jan 09 19:13:...|http://www.crunch...|            17|               7|              |           2011|scan, copy, and f...|      info@scanR.com|                  []|       null|         null|        2005|[[23,4,2009,7175,...|http://www.scanr.com|[null,WrappedArra...|         []|null|        []|       scanR|               null|[[100 Hamilton,Su...|<p>scanR, Inc. de...|      []|       scanr|650-853-3080|                  []|                  []|[[true,[Rudy,Ruan...|                  []|                null|            $14.2M|           scanR|Thu Oct 03 07:32:...|          []|
+--------------------+-----------+------------+----------+-------------+--------------------+-------------+------------+--------------------+--------------------+--------------+----------------+--------------+---------------+--------------------+--------------------+--------------------+-----------+-------------+------------+--------------------+--------------------+--------------------+-----------+----+----------+------------+-------------------+--------------------+--------------------+--------+------------+------------+--------------------+--------------------+--------------------+--------------------+--------------------+------------------+----------------+--------------------+------------+


   */
    // DataFrame Query: SQL Group By
    println("Group by CategoryCode value")
    df.groupBy("category_code").count().show(10)
/* Output:
  +----------------+-----+
    |   category_code|count|
    +----------------+-----+
    |          travel|   25|
      |public_relations|  533|
      |           local|    1|
      |     advertising|  928|
      |      automotive|    9|
      |         finance|   49|
      |     photo_video|   23|
      | network_hosting|  626|
      |           legal|   25|
      |            news|   48|
      +----------------+-----+
    only showing top 10 rows*/

    compNameDF.show()

    /*  Output:
    +--------------+------------+
    |          name|founded_year|
    +--------------+------------+
    |      Wetpaint|        2005|
    |        Scribd|        2007|
    |         Plaxo|        2002|
    |       adBrite|        2003|
    |          ooma|        2004|
    |    Socialtext|        2002|
    |       ONEsite|        2005|
    |      Wallhogs|        2006|
    |       boo-box|        2007|
    |          Echo|        2007|
    |     Six Apart|        2001|
    |        OMGPOP|        2006|
    |   Crunchyroll|        2008|
    |         4INFO|        2004|
    | Jive Software|        2001|
    |         WooMe|        2000|
    |    BitGravity|        2006|
    |        Peer39|        2006|
    |Lijit Networks|        2006|
    |  ChoiceStream|        2001|
    +--------------+------------+  */
    //df.show()
  }
}