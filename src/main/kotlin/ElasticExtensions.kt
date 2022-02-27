import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.search.Hit
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

