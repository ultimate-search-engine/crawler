import io.ktor.http.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import libraries.Address
import libraries.Credentials
import libraries.Elastic
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.exitProcess

//val IndexName = System.getenv("IndexName") ?: "search"
//val Host = System.getenv("ElasticHost") ?: "localhost"
//val Password = System.getenv("ELASTIC_PASSWORD") ?: throw (Exception("ELASTIC_PASSWORD not set in env variable"))
//val esPort = System.getenv("ES_PORT").toIntOrNull() ?: 9200
//val CrawlerUrl = Url(System.getenv("CrawlerUrl") ?: throw (Exception("CrawlerUrl needed in env variable")))

suspend fun main(args: Array<String>): Unit = runBlocking {

    runCrawler()

    exitProcess(0)
}


suspend fun runCrawler() {

    val crawler = Crawler(listOf(PageScraper("http://localhost:8080/crawler", AtomicInteger(6))), "wiki2")
//    crawler.indexFirstPage(Url("https://en.wikipedia.org/"))
    crawler.crawl()
}


