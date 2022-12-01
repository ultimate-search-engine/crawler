import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.exitProcess

import io.github.cdimascio.dotenv.dotenv

//val IndexName = dotenv["IndexName") ?] "search"
//val Host = dotenv["ElasticHost") ?] "localhost"
//val Password = dotenv["ELASTIC_PASSWORD") ?] throw (Exception("ELASTIC_PASSWORD not set in env variable"))
//val esPort = dotenv["ES_PORT").toIntOrNul]() ?: 9200
//val CrawlerUrl = Url(dotenv["CrawlerUrl") ?] throw (Exception("CrawlerUrl needed in env variable")))

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

val dotenv = dotenv()

val dbHost: String = (dotenv["DB_HOST"] ?: "").ifEmpty { "localhost" }
val dbPort: String = (dotenv["DB_PORT"] ?: "").ifEmpty { "27017" }
val dbUsername = (dotenv["DB_USERNAME"] ?: "").ifEmpty { "root" }
val dbPassword = (dotenv["DB_PASSWORD"] ?: "").ifEmpty { "root" }
val envLimit = (dotenv["LIMIT"] ?: "").ifEmpty { "100000" }.toLong()
val boostForUrls = (dotenv["URLS_BOOST"] ?: "").ifEmpty { "1.0,0.2,1.0,5.0,16.0,32.0,4.0" }.split(",").map { it.toDouble() }
