import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*
import io.ktor.client.features.json.serializer.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import libraries.PageRepository
import org.jsoup.Jsoup
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@Serializable
data class PageScraperResponse(val html: String, val status: Int, val url: String)

@Serializable
data class PageScraperRequest(val url: String)

data class PageScraper(
    val url: String,
    val maxConcurrentRequests: AtomicInteger = AtomicInteger(0),
    var currentConcurrency: AtomicInteger = AtomicInteger(0)
)

class Crawler(private val pageScrapers: List<PageScraper>, dbName: String, private val limit: Long) {
    private val docsIndexed = AtomicLong(0)

    private val urlDecider: UrlDecide = UrlDecider()
    private val isWanted = IsWanted()

    private val ktor = HttpClient(CIO) {
        install(JsonFeature) {
            serializer = KotlinxSerializer()
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 80_000
        }
    }


    private fun getAvailableScraper() =
        pageScrapers.minByOrNull { it.currentConcurrency.get().toDouble() / it.maxConcurrentRequests.get() }?.let {
            if (it.currentConcurrency.get() < it.maxConcurrentRequests.get()) it
            else null
        }

    private suspend fun scrapePage(url: Url, nthTry: Int = 0): PageScraperResponse? = coroutineScope {
        // selects the least busy scraper
        val scraper = getAvailableScraper() ?: throw Exception("No scraper available")

        scraper.currentConcurrency.incrementAndGet()
        return@coroutineScope try {
            val post: PageScraperResponse = ktor.post(scraper.url) {
                contentType(ContentType.Application.Json)
                body = PageScraperRequest(url.cUrl())
            }
            scraper.currentConcurrency.decrementAndGet()
            post
        } catch (e: Exception) {
            scraper.currentConcurrency.decrementAndGet()
            println("Error during POST request with url: $url, retrying in 10 seconds")
            delay(10_000)
            if (nthTry < 3) scrapePage(url, nthTry + 1)
            else null
        }
    }


    suspend fun crawl() = coroutineScope {
        val concurrency = pageScrapers.sumOf { it.maxConcurrentRequests.get() }
        for (i in 1..concurrency) {
            launch(Dispatchers.Unconfined) {
                while (docsIndexed.get() < limit) {
                    val randomPage = urlDecider.get()
                    handlePage(randomPage.first, randomPage.second.dbName)
                    urlDecider.free(randomPage.first)
                }
            }
        }
    }

    private suspend fun handlePage(randomUrl: Url, dbClient: PageRepository.Client) {
        if ((docsIndexed.getAndIncrement() % 200) == 0L) {
            println("${getAvailableScraper()?.currentConcurrency?.get()}/${getAvailableScraper()?.maxConcurrentRequests?.get()}")
            println("${docsIndexed.get()}/${limit}")
        }

        val page = scrapePage(randomUrl) ?: return

        val prev = dbClient.find(Url(page.url).cUrl())
        val doc = Jsoup.parse(page.html)
        val code = if (page.status == 200) {
            if (isWanted.isWanted(doc)) page.status else 0
        } else page.status

        try {
            handlePageWrite(randomUrl, page, code, prev, dbClient)
        } catch (e: Exception) {
            println("Error: ${e.message}")
        }
    }


    private suspend fun handlePageWrite(
        randomUrl: Url,
        page: PageScraperResponse,
        code: Int,
        prev: PageRepository.Page? = null,
        dbClient: PageRepository.Client
    ) {
//        println("$code ${randomUrl.cUrl()}")
        if (prev != null) {
            dbClient.update(
                PageRepository.Page(
                    (prev.targetUrl + randomUrl.cUrl()).distinct(),
                    Url(page.url).cUrl(),
                    page.html,
                    System.currentTimeMillis(),
                    code
                )
            )
        } else {
            dbClient.add(
                PageRepository.Page(
                    listOf(randomUrl.cUrl()), Url(page.url).cUrl(), page.html, System.currentTimeMillis(), code
                )
            )
        }
    }


    suspend fun indexFirstPage(url: Url, client: PageRepository.Client) {
        val res = scrapePage(url) ?: throw Exception("Could not scrape page")
        if (res.status == 200) {
            handlePageWrite(url, res, 200, dbClient = client)

            println("Indexed $url successfully")
        } else println("Url cannot be crawled")
    }


}


fun Url.cUrl(): String {
    val url = "${this.protocol.name}://${this.host}${this.encodedPath}"
    if (url.endsWith("/")) return url.dropLast(1)
    return url
}
