import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.search.Hit
import htmlParser.Url
import kotlinx.coroutines.delay
import libraries.Address
import libraries.Credentials
import libraries.Elastic
import libraries.Page

suspend fun Elastic.maxValueByFieldAndCrawlerStatus(
    field: String,
    crawlerStatus: Page.CrawlerStatus,
    batchSize: Long = 10,
): List<Hit<Page.PageType>>? =
    search(SearchRequest.of {
        it.index(index)
        it.sort { sort ->
            sort.field { fieldSort ->
                fieldSort.field(field)
                fieldSort.order(SortOrder.Desc)
            }
        }
        it.size(batchSize.toInt())
        it.query { query ->
            query.bool { bool ->
                bool.must { must ->
                    must.term { term ->
                        term.field("crawlerStatus").value { termValue ->
                            termValue.stringValue(crawlerStatus.toString())
                        }
                    }
                }
            }
        }
    }).hits().hits()


suspend fun createIndexWithInitialPage() {
    val es = Elastic(Credentials("elastic", Password), Address(Host, 9200), "$IndexName${System.currentTimeMillis()}")

    println("Creating index")
    try {
        es.alias.delete(IndexName)
    } catch (e: Exception) {
        println("Alias $IndexName did not exist")
    }
    es.putMapping(6)
    es.alias.create(IndexName)
    val crawler = Crawler(es, 100, CrawlerUrl)
    crawler.indexFirstPage(Url("https://github.com"))
    delay(1000)
}