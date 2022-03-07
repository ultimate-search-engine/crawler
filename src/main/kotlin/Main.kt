import htmlParser.Url
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import libraries.Address
import libraries.Credentials
import libraries.Elastic

const val IndexName = "search"
const val Host = "10.0.0.33"
const val Password = "0Se+4Stcs4VGWYBzLKip"
val CrawlerUrl = Url("https://page-scraper-4tm6hxrtia-ew.a.run.app/crawler")

suspend fun main(args: Array<String>) = runBlocking {

    val es = Elastic(Credentials("elastic", Password), Address(Host, 9200), IndexName)

    if (args.isNotEmpty() && args[0] == "createIndex") createIndexWithInitialPage()

    runCrawler(es)
}


suspend fun runCrawler(es: Elastic) {
    val docCount = es.getAllDocsCount()
    val maxBatchSize = 800L

    val size = listOf(docCount / 15, maxBatchSize).minOf { it }.coerceAtLeast(42)
    val batches = ((docCount / 15) / maxBatchSize)
    println("$docCount docs, $batches batches, $size docs per batch")

    for (i in 0..batches) {
        println("$i batch")
        println("Scraping $size documents")

        val crawler = Crawler(es, size, CrawlerUrl)
        crawler.crawl(16)

        delay(2000)
    }
}



