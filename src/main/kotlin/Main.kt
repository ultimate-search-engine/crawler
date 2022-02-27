import htmlParser.Url
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import libraries.Address
import libraries.Credentials
import libraries.Elastic


suspend fun main() = runBlocking {

//    val crawler = Crawler("search", 100, Url("https://page-scraper-4tm6hxrtia-ew.a.run.app/crawler"))
//    val es = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), "search${System.currentTimeMillis()}")
//
//    es.alias.getIndexByAlias("search").forEach { es.deleteIndex(it) }
//    es.putMapping(3)
//    es.alias.create("search")
//
//    crawler.indexFirstPage(Url("https://github.com"))

//    for (i in 0..10) {
//    println("$i batch")
//    delay(5000)

    val es = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), "search")
    val docCount = es.getAllDocsCount()
    val size = listOf(docCount / 10, 600).minOf { it }
    println("Scraping $size documents")
    val crawler = Crawler("search", size, Url("https://page-scraper-4tm6hxrtia-ew.a.run.app/crawler"))
    crawler.crawl(8)

}
