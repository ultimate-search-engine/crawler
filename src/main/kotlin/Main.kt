import HtmlParser.Url
import kotlinx.coroutines.runBlocking


suspend fun main() = runBlocking {
    val crawler = Crawler("search", 100, Url("https://page-scraper-4tm6hxrtia-ew.a.run.app/crawler"))

//    val es = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), "search${System.currentTimeMillis()}")
//    es.alias.getIndexByAlias("search").forEach { es.deleteIndex(it) }
//    es.putMapping(3)
//    es.alias.create("search")
//    crawler.indexFirstPage(Url("https://github.com"))

    crawler.crawl()
}
