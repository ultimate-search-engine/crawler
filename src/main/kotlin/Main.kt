import htmlParser.Url
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import libraries.Address
import libraries.Credentials
import libraries.Elastic
import kotlin.system.exitProcess

val IndexName = System.getenv("IndexName") ?: "search"
val Host = System.getenv("ElasticHost") ?: "localhost"
val Password = System.getenv("ELASTIC_PASSWORD") ?: throw(Exception("ELASTIC_PASSWORD not set in env variable"))
val esPort = System.getenv("ES_PORT").toIntOrNull() ?: 9200
val CrawlerUrl = Url(System.getenv("CrawlerUrl") ?: throw(Exception("CrawlerUrl needed in env variable")))
const val bulkSize = 300

suspend fun main(args: Array<String>): Unit = runBlocking {

    val es = Elastic(Credentials("elastic", Password), Address(Host, esPort), IndexName)

    if (args.isNotEmpty() && args[0] == "init") {
        createIndexWithInitialPage()
    } else {
        runCrawler(es)
    }
    
    exitProcess(0)
}


suspend fun runCrawler(es: Elastic) {
    val docCount = es.getAllDocsCount()
    val maxBatchSize = 4000L

    val size = listOf(docCount / 15, maxBatchSize).minOf { it }.coerceAtLeast(42)
    val batches = ((docCount / 15) / maxBatchSize).coerceAtMost(42*6).coerceAtLeast(1)
    println("$docCount docs, $batches batches, $size docs per batch")

    for (i in 1..batches) {
        println("$i batch")
        println("Scraping $size documents...")

        val crawler = Crawler(es, size, CrawlerUrl)
        crawler.crawl(10)

        delay(3500)
    }
}

