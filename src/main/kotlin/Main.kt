import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.exitProcess

//val IndexName = System.getenv("IndexName") ?: "search"
//val Host = System.getenv("ElasticHost") ?: "localhost"
//val Password = System.getenv("ELASTIC_PASSWORD") ?: throw (Exception("ELASTIC_PASSWORD not set in env variable"))
//val esPort = System.getenv("ES_PORT").toIntOrNull() ?: 9200
//val CrawlerUrl = Url(System.getenv("CrawlerUrl") ?: throw (Exception("CrawlerUrl needed in env variable")))

suspend fun main(): Unit = runBlocking {

    runCrawler()

    exitProcess(0)

}


suspend fun runCrawler() {

    val crawler =
        Crawler(
            listOf(
                PageScraper(
                    "http://localhost:8080/crawler",
                    AtomicInteger(6)
                )
            ), "ency", limit = Long.MAX_VALUE
        )

//    allowedCrawlerHosts.forEach {
//        crawler.indexFirstPage(it.host, it.dbName)
//    }
//    crawler.indexFirstPage(allowedCrawlerHosts[5].host, allowedCrawlerHosts[5].dbName)
    crawler.crawl()
}


