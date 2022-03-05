import htmlParser.Url
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import libraries.Address
import libraries.Credentials
import libraries.Elastic

const val INDEX_NAME = "search"
const val HOST = "10.0.0.33"
const val PASSWORD = "0Se+4Stcs4VGWYBzLKip"

suspend fun main() = runBlocking {

    val es = Elastic(Credentials("elastic", PASSWORD), Address(HOST, 9200), INDEX_NAME)

//    val crawler = Crawler(es, 100, Url("https://page-scraper-4tm6hxrtia-ew.a.run.app/crawler"))
//
//    es.alias.getIndexByAlias("search").forEach { es.deleteIndex(it) }
//    es.putMapping(3)
//    es.alias.create("search")
//
//    crawler.indexFirstPage(Url("https://github.com"))

    val docCount = es.getAllDocsCount()
    val maxBatchSize = 800L

    val size = listOf(docCount / 15, maxBatchSize).minOf { it }.coerceAtLeast(42)
    val batches = ((docCount / 15) / maxBatchSize)
    println("$docCount docs, $batches batches, $size docs per batch")

    for (i in 0..batches) {
        println("$i batch")
        println("Scraping $size documents")

        val crawler = Crawler(es, size, Url("https://page-scraper-4tm6hxrtia-ew.a.run.app/crawler"))
        crawler.crawl(16)

        delay(2000)
    }

}
