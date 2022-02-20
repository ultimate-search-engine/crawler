import HtmlParser.Url
import kotlinx.coroutines.runBlocking
import libraries.Address
import libraries.Credentials
import libraries.Elastic


suspend fun main() = runBlocking {
    val crawler = Crawler("search", 80, Url("https://page-scraper-4tm6hxrtia-ew.a.run.app/crawler"))

//    val es = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), "search${System.currentTimeMillis()}")
//    es.alias.getIndexByAlias("search").forEach { es.deleteIndex(it) }
//    es.putMapping()
//    es.alias.create("search")
//    crawler.startIndex(Url("https://github.com"))

    crawler.crawl()
}
