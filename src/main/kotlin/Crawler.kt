import co.elastic.clients.elasticsearch.core.search.Hit
import htmlParser.HtmlParser
import htmlParser.Url
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
import libraries.*
import java.util.concurrent.atomic.AtomicLong

@Serializable
data class PageScraperResponse(val html: String?, val status: Int, val url: String)

@Serializable
data class PageScraperRequest(val url: String)


class Crawler(index: String, private val docCount: Long, private val pageScraperUrl: Url) {
    private val es = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), index)
    private val indexObj = IndexQueue(es)

    private val ktor = HttpClient(CIO) {
        install(JsonFeature) {
            serializer = KotlinxSerializer()
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 40000
        }
    }


    private var currentlyIndexingCount = AtomicLong(0)
    private var finishedIndexing = AtomicLong(0)

    private val queue = mutableMapOf<String, List<Hit<Page.PageType>>>()

    private fun prepareQueue(docs: List<Hit<Page.PageType>>) {
        for (doc in docs) {
            val url = doc.source()?.address?.url ?: continue
            val domain = getDomain(url)

            queue[domain] = queue[domain]?.plus(doc) ?: listOf(doc)


        }
    }


    private suspend fun queueDocs() {
        val docs =
            es.maxValueByFieldAndCrawlerStatus("inferredData.ranks.pagerank", Page.CrawlerStatus.NotCrawled, docCount)
        if (docs?.isNotEmpty() != null) {
            prepareQueue(docs)
        }
    }


    private suspend fun scrapePage(url: Url): PageScraperResponse = ktor.post(pageScraperUrl.get()) {
        contentType(ContentType.Application.Json)
        body = PageScraperRequest(url.get())
    }


    private suspend fun crawlDomain(domain: String) {
        val docs = queue[domain] ?: return
        docs.forEach { doc ->
            val source = doc.source() ?: return@forEach
            println("Scraping ${source.address.url}")
            try {
                if (queue.keys.count() - finishedIndexing.get() < queue.keys.count() / 10) {
                    // if 90% of domains are finished, stops crawling
                    return@crawlDomain
                }
                val res = scrapePage(Url(source.address.url))
                if (res.html != null) {
                    val page = HtmlParser(
                        if (res.status == 200) res.html else "",
                        Url(source.address.url),
                        source.inferredData.backLinks
                    )
                    if (res.status == 200) page.crawlerStatus = Page.CrawlerStatus.Crawled
                    if (res.status == 400) page.crawlerStatus = Page.CrawlerStatus.Error
                    if (res.status == 404) page.crawlerStatus = Page.CrawlerStatus.DoesNotExist

                    indexObj.add(page)
                }
            } catch (e: Exception) {
                println("Error scraping ${source.address.url}")
                println(e.message)
            }
        }
        finishedIndexing.incrementAndGet()
    }


    suspend fun crawl(concurrencyLimit: Int) = coroutineScope {
        queueDocs()

        queue.map {
            while (currentlyIndexingCount.get() >= concurrencyLimit) delay(100)
            currentlyIndexingCount.incrementAndGet()
            launch(Dispatchers.Unconfined) {
                crawlDomain(it.key)
                currentlyIndexingCount.decrementAndGet()
            }
        }.forEach { it.join() }

        val foo = indexObj.mapToDocs()
        es.indexDocsBulkByIds(foo)

        println("Crawler finished")
    }

    suspend fun indexFirstPage(url: Url) {
        val res = scrapePage(url)
        if (res.status == 200 && res.html != null) {
            val indexObj = IndexQueue(es)
            val page = HtmlParser(res.html, Url(res.url), listOf())
            println(page.metadata.title)
            page.inferredData.ranks.smartRank = 1.0
            indexObj.add(page)
            val foo = indexObj.mapToDocs()
            es.indexDocsBulkByIds(foo)

            println("Indexed ${url.get()} successfully")
        } else println("Url cannot be crawled")
        delay(2000)
    }

}