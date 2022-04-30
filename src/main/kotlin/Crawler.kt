import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*
import io.ktor.client.features.json.serializer.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import libraries.PageRepository
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import java.util.*
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
    private val dbClient = PageRepository.MongoClient(dbName, "mongodb://localhost:27017")
    private val limit = limit ?: Long.MAX_VALUE
    private val docsIndexed = AtomicLong(0)

    private val currentlyCrawling = CurrentlyCrawling()

    private val ktor = HttpClient(CIO) {
        install(JsonFeature) {
            serializer = KotlinxSerializer()
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 42_000
        }
    }

    private class CurrentlyCrawling {
        private val currentlyCrawling = mutableSetOf<String>()

        @OptIn(DelicateCoroutinesApi::class)
        private val context = newSingleThreadContext("CurrentlyCrawling")
        suspend fun add(url: String) = withContext(context) { currentlyCrawling.add(url) }

        suspend fun remove(url: String) = withContext(context) { currentlyCrawling.remove(url) }

        suspend fun contains(url: String) = withContext(context) { currentlyCrawling.contains(url) }

        suspend fun addDomain(url: Url) = withContext(context) { currentlyCrawling.add(url.host) }

        suspend fun removeDomain(url: Url) = withContext(context) { currentlyCrawling.remove(url.host) }

        suspend fun containsDomain(url: Url) = withContext(context) { currentlyCrawling.contains(url.host) }

    }


    private fun getAvailableScraper() =
        pageScrapers.minByOrNull { it.currentConcurrency.get().toDouble() / it.maxConcurrentRequests.get() }?.let {
            if (it.currentConcurrency.get() < it.maxConcurrentRequests.get()) it
            else null
        }

    private suspend fun scrapePage(url: Url): PageScraperResponse = coroutineScope {
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
            scrapePage(url)
        }
    }


    suspend fun crawl() = coroutineScope {
        val concurrency = pageScrapers.sumOf { it.maxConcurrentRequests.get() }
        for (i in 1..concurrency) {
            launch(Dispatchers.Unconfined) {
                while (docsIndexed.get() < limit) {
                    val randomPage = getRandomNotCrawledPage()
                    handlePage(randomPage)
                }
            }
        }
    }

    private suspend fun handlePage(randomUrl: Url) {
        if (randomUrl.protocol.name == "http") return
        if ((docsIndexed.getAndIncrement() % 200) == 0L) {
            println("${getAvailableScraper()?.currentConcurrency?.get()}/${getAvailableScraper()?.maxConcurrentRequests?.get()}")
            println("${docsIndexed.get()}/${limit}")
        }

        currentlyCrawling.add(randomUrl.cUrl())
        currentlyCrawling.addDomain(randomUrl)
        val page = scrapePage(randomUrl)
        val prev = dbClient.find(Url(page.url).cUrl()).firstOrNull()

        val doc = Jsoup.parse(page.html)

        val code = if (page.status == 200) {
            if (isWanted.isWanted(doc)) page.status else 0
        } else page.status

        try {
            handlePageWrite(randomUrl, page, code, prev)
        } catch (e: Exception) {
            println("Error: ${e.message}")
        }

        currentlyCrawling.removeDomain(randomUrl)
        if (code == 200) dampeningUrl(doc, randomUrl)
        currentlyCrawling.remove(randomUrl.cUrl())
    }

    private suspend fun dampeningUrl(doc: Document, url: Url) {
        val xd = doc.pageLinks(url).randomOrNull()
        if (xd != null) {
            val random = Random()
            if (random.nextFloat() < 0.85) {
                if (!isIndexed(xd)) {
                    handlePage(xd)
                } else {
                    (dbClient.find(xd.cUrl()).firstOrNull() ?: dbClient.findTarget(xd.cUrl()).firstOrNull())?.let {
                        dampeningUrl(Jsoup.parse(it.content), Url(it.finalUrl))
                    }
                }
            }
        }
    }


    private val isWanted = IsWanted()

    private suspend fun handlePageWrite(
        randomUrl: Url,
        page: PageScraperResponse,
        code: Int,
        prev: PageRepository.Page? = null
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


    private tailrec suspend fun getRandomNotCrawledPage(): Url {
        val randomPageDoc = dbClient.randomPages(1, 200).firstOrNull() ?: return getRandomNotCrawledPage()
        val randomPageHtml = Jsoup.parse(randomPageDoc.content)
        val link =
            randomPageHtml.pageLinks(Url(randomPageDoc.finalUrl)).randomOrNull() ?: return getRandomNotCrawledPage()
        return if (!isIndexed(link)) link
        else getRandomNotCrawledPage()
    }

    private suspend fun isIndexed(link: Url): Boolean {
        return currentlyCrawling.contains(link.cUrl())
                || currentlyCrawling.containsDomain(link)
                || dbClient.find(link.cUrl()).isNotEmpty()
                || dbClient.findTarget(link.cUrl()).isNotEmpty()
    }

    suspend fun indexFirstPage(url: Url) {
        val res = scrapePage(url)
        if (res.status == 200) {
            handlePageWrite(url, res, 200)

            println("Indexed $url successfully")
        } else println("Url cannot be crawled")
    }


}


fun Document.pageLinks(url: Url): List<Url> {
    val links = this.select("a")

    return links.mapNotNull {
        try {
            val href = it.attr("href")
            if (href.startsWith("https")) {
                Url(href)
            } else if (href.startsWith("/") && !href.startsWith("//")) {

                val hrefUrl = "${url.protocol.name}://${url.host}$href"

                if (hrefUrl.endsWith("/")) Url(hrefUrl.dropLast(1))
                else Url(hrefUrl)

            } else {
                null
            }
        } catch (e: Exception) {
            null
        }
    }

}


fun Url.cUrl(): String {
    val url = "${this.protocol.name}://${this.host}${this.encodedPath}"
    if (url.endsWith("/")) return url.dropLast(1)
    return url
}
