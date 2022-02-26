import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.SearchResponse
import co.elastic.clients.elasticsearch.core.search.Hit
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import libraries.Address
import libraries.Credentials
import libraries.Elastic
import libraries.Page

class ElasticExtended(credentials: Credentials, address: Address, index: String): Elastic(credentials, address, index) {
    
    suspend fun maxValueByFieldAndCrawlerStatus(
        field: String,
        crawlerStatus: Page.CrawlerStatus,
        batchSize: Long = 10,
    ): List<Hit<Page.PageType>>? = coroutineScope {
        val search: SearchResponse<Page.PageType> = withContext(Dispatchers.Default) {
            client.search(SearchRequest.of {
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
            }, Page.PageType::class.java)
        }
        return@coroutineScope search.hits().hits()
    }
    
    
}