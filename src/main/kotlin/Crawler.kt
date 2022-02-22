import HtmlParser.HtmlParser
import HtmlParser.Url
import co.elastic.clients.elasticsearch.core.search.Hit
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

@Serializable
data class PageScraperResponse(val html: String?, val status: Int, val url: String)

@Serializable
data class PageScraperRequest(val url: String)


class Crawler(index: String, private val amount: Long, private val pageScraperUrl: Url) {
    private val es = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), index)
    private val ktor = HttpClient(CIO) {
        install(JsonFeature) {
            serializer = KotlinxSerializer()
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 40000
        }
    }


    private var currentlyIndexingCount: Long = 0

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
            es.maxValueByFieldAndCrawlerStatus("inferredData.ranks.pagerank", Page.CrawlerStatus.NotCrawled, amount)
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
            print("Crawling ${source.address.url}")
            val res = scrapePage(Url(source.address.url))

            if (res.status == 200 && res.html != null) {
                val page = HtmlParser(res.html, Url(source.address.url), source.inferredData.backLinks)

                println(" -> Indexing backlinks (${page.body.links.internal.count() + page.body.links.external.count()})")
                putDocsBacklinkInfoByUrl(page.body.links.internal, cleanUrl(res.url))
                putDocsBacklinkInfoByUrl(page.body.links.external, cleanUrl(res.url))
                es.indexPage(page, doc.id())
            }
            else if (res.status == 404) {
                println("Does not exist: ${res.status}")
                val page = HtmlParser("", Url(source.address.url), source.inferredData.backLinks)
                page.crawlerStatus = Page.CrawlerStatus.DoesNotExist
                es.indexPage(page, doc.id())
            }
            else {
                println("Error: ${res.status}")
                val page = HtmlParser("", Url(source.address.url), source.inferredData.backLinks)
                page.crawlerStatus = Page.CrawlerStatus.Error
                es.indexPage(page, doc.id())
            }
        }
        currentlyIndexingCount -= 1
    }

    private suspend fun putDocsBacklinkInfoByUrl(
        docForwardLinks: List<Page.ForwardLink>,
        originUrl: String,
    ): Unit = coroutineScope {
        docForwardLinks.mapNotNull { docForwardLink ->
            if (docForwardLink.href != originUrl) {
                launch(Dispatchers.Unconfined) {
                    es.putDocBacklinkInfoByUrl(
                        cleanUrl(docForwardLink.href),
                        Page.BackLink(docForwardLink.text, originUrl),
                    )
                }
            } else null
        }.forEach { it.join() }
    }


    suspend fun crawl() = coroutineScope {
        queueDocs()
        for (item in queue) {
            currentlyIndexingCount += 1
            crawlDomain(item.key)
        }
        println("Crawler finished")
    }

    suspend fun indexFirstPage(url: Url) {
        val res = scrapePage(url)
        if (res.status == 100 && res.html != null) {
            val page = HtmlParser(res.html, Url(res.url), listOf())
            println(page.metadata.title)
            page.inferredData.ranks.smartRank = 1.0
            es.indexPage(page)
            putDocsBacklinkInfoByUrl(page.body.links.internal, res.url)
            putDocsBacklinkInfoByUrl(page.body.links.external, res.url)
            println("Indexed ${url.get()} successfully")
        }
        else println("Url cannot be crawled")
        delay(2000)
    }

}