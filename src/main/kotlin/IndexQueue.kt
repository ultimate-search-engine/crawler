import kotlinx.coroutines.*
import libraries.Elastic
import libraries.Page

class IndexQueue(private val elastic: ElasticExtended) {
    private val pages = mutableMapOf<String, Page.PageType>()
    @OptIn(DelicateCoroutinesApi::class)
    private val queueContext = newSingleThreadContext("index-queue")

    suspend fun add(page: Page.PageType) = coroutineScope {
        withContext (queueContext) {
            parsePage(page)
            parsePageLinks(page)
        }
    }

    private fun parsePage(page: Page.PageType) {
        val pageMapElem = pages[page.address.url]
        page.inferredData.backLinks = pageMapElem?.inferredData?.backLinks ?: listOf()
        pages[page.address.url] = page
    }

    private fun parsePageLinks(page: Page.PageType) {
        for (link in page.body.links.external + page.body.links.internal) {
            val pageMapElem = pages[link.href]
            if (pageMapElem != null) {
                pageMapElem.inferredData.backLinks += Page.BackLink(link.text, page.address.url)
                pages[link.href] = pageMapElem
            } else {
                val newPage = Page.PageType(link.href)
                newPage.inferredData.backLinks += Page.BackLink(link.text, page.address.url)
                pages[link.href] = newPage
            }
        }
    }

    suspend fun mapToDocs(): List<Elastic.PageById> {
        val bulkSearch = withContext(Dispatchers.IO) { elastic.docsByUrlOrNullBulk(pages.keys.toList()) }

        val responses = bulkSearch?.responses()?.map { it.result().hits().hits().firstOrNull() } ?: listOf()
        val zipped = responses.zip(pages.values)

        return zipped.map { (hit, page) ->
            if (hit != null) {
                if (page.crawlerStatus != Page.CrawlerStatus.NotCrawled) { // if newly crawled
                    val source = hit.source() ?: page
                    page.inferredData.backLinks = (page.inferredData.backLinks + source.inferredData.backLinks).distinctBy { it.source }
                    Elastic.PageById(page, hit.id())
                } else {
                    val source = hit.source() ?: page
                    source.inferredData.backLinks = (page.inferredData.backLinks + source.inferredData.backLinks).distinctBy { it.source }
                    Elastic.PageById(source, hit.id())
                }
            } else {
                Elastic.PageById(page, null)
            }
        }
    }
}