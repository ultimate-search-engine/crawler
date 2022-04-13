import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*
import io.ktor.client.features.json.serializer.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import libraries.*
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
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

class Crawler(private val pageScrapers: List<PageScraper>, dbName: String, limit: Long? = Long.MAX_VALUE) {
    private val dbClient = PageRepository(dbName).mongoClient()
    private val docsLeft = AtomicLong(limit ?: Long.MAX_VALUE)

    private val currentlyCrawling = CurrentlyCrawling()

    private val ktor = HttpClient(CIO) {
        install(JsonFeature) {
            serializer = KotlinxSerializer()
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 60_000
        }
    }

    private class CurrentlyCrawling {
        private val currentlyCrawling = mutableSetOf<String>()

        @OptIn(DelicateCoroutinesApi::class)
        private val context = newSingleThreadContext("CurrentlyCrawling")
        suspend fun add(url: String) = withContext(context) { currentlyCrawling.add(url) }

        suspend fun remove(url: String) = withContext(context) { currentlyCrawling.remove(url) }

        suspend fun contains(url: String) = withContext(context) { currentlyCrawling.contains(url) }

    }


    private fun getAvailableScraper() =
        pageScrapers.minByOrNull { it.currentConcurrency.get().toDouble() / it.maxConcurrentRequests.get() }?.let {
            if (it.currentConcurrency.get() < it.maxConcurrentRequests.get()) it
            else null
        }

    private suspend fun scrapePage(url: Url): PageScraperResponse {
        // selects the least busy scraper
        val scraper = getAvailableScraper() ?: throw Exception("No scraper available")

        scraper.currentConcurrency.incrementAndGet()
        val post: PageScraperResponse = ktor.post(scraper.url) {
            contentType(ContentType.Application.Json)
            body = PageScraperRequest(url.basicUrl())
        }
        scraper.currentConcurrency.decrementAndGet()
        return post
    }


    suspend fun crawl() = coroutineScope {
        val concurrency = pageScrapers.sumOf { it.maxConcurrentRequests.get() }
        for (i in 1..concurrency) {
            launch(Dispatchers.Unconfined) {
                while (docsLeft.get() > 0) {
                    val randomPage = getRandomNotCrawledPage()
                    println("url: $randomPage")

                    currentlyCrawling.add(randomPage.basicUrl())

                    val page = scrapePage(randomPage)
                    val prev = dbClient.find(Url(page.url).basicUrl()).firstOrNull()
                    handlePageWrite(randomPage, page, prev)

                    currentlyCrawling.remove(randomPage.basicUrl())
                }
            }
        }
    }


    private val isWanted = IsWanted()

    private suspend fun handlePageWrite(randomPage: Url, page: PageScraperResponse, prev: PageRepository.Page? = null) {
        val doc = Jsoup.parse(page.html)

        val code = if (page.status == 200) {
            if (isWanted.isWanted(doc)) page.status else 0
        } else page.status

        if (prev != null) {
            dbClient.update(
                PageRepository.Page(
                    prev.targetUrl + randomPage.basicUrl(),
                    Url(page.url).basicUrl(),
                    page.html,
                    System.currentTimeMillis(),
                    code
                )
            )
        } else {
            dbClient.add(
                PageRepository.Page(
                    listOf(randomPage.basicUrl()), Url(page.url).basicUrl(), page.html, System.currentTimeMillis(), code
                )
            )
        }
    }


    private tailrec suspend fun getRandomNotCrawledPage(): Url {
        val randomPageDoc = dbClient.randomPages(1).firstOrNull() ?: return getRandomNotCrawledPage()
        val randomPageHtml = Jsoup.parse(randomPageDoc.content)
        val randomUrl =
            randomPageHtml.pageLinks(Url(randomPageDoc.finalUrl)).randomOrNull() ?: return getRandomNotCrawledPage()
        val link = Url(randomUrl)
        return if (!currentlyCrawling.contains(link.basicUrl()) && dbClient.find(link.basicUrl())
                .isEmpty() && dbClient.findTarget(link.basicUrl())
                .isEmpty() && link.host == Url("https://en.wikipedia.org").host
        ) link
        else getRandomNotCrawledPage()
    }

    suspend fun indexFirstPage(url: Url) {
        val res = scrapePage(url)
        if (res.status == 200) {
            handlePageWrite(url, res)

            println("Indexed $url successfully")
        } else println("Url cannot be crawled")
    }


}


fun Document.pageLinks(url: Url): List<String> {
    val links = this.select("a")

    return links.mapNotNull {
        val href = it.attr("href")
        if (href.startsWith("https")) {
            href
        } else if (href.startsWith("/") && !href.startsWith("//")) {
            "${url.protocol.name}://${url.host}$href"
        } else {
            null
        }
    }

}


fun Url.basicUrl(): String = "${this.protocol.name}://${this.host}${this.encodedPath}"
