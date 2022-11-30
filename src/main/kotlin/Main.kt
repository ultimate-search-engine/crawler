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
            ), "ency", limit = envLimit
        )

//    allowedCrawlerHosts.forEach {
//        crawler.indexFirstPage(it.host, it.dbName)
//    }
//    crawler.indexFirstPage(allowedCrawlerHosts[5].host, allowedCrawlerHosts[5].dbName)
    crawler.crawl()
}

val dbHost: String = (System.getenv("DB_HOST") ?: "").ifEmpty { "localhost" }
val dbPort: String = (System.getenv("DB_PORT") ?: "").ifEmpty { "27017" }
val dbUsername = (System.getenv("DB_USERNAME") ?: "").ifEmpty { "" }
val dbPassword = (System.getenv("DB_PASSWORD") ?: "").ifEmpty { "" }
val envLimit = (System.getenv("LIMIT") ?: "").ifEmpty { "100000" }.toLong()
val boostForUrls = (System.getenv("DB_PORT") ?: "").ifEmpty { "1.0,0.2,1.0,5.0,16.0,32.0,4.0" }.split(",").map { it.toDouble() }
