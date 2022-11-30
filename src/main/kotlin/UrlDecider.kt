import io.ktor.http.*
import kotlinx.coroutines.*
import libraries.PageRepository
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

abstract class UrlDecide {
    abstract suspend fun get(): Pair<Url, Host>
    abstract suspend fun free(url: Url)
}

data class Host(
    val host: Url,
    val boost: Double,
    val dbName: PageRepository.Client,
    val fn: (url: Url) -> Boolean,
    var crawledSoFar: Long = 0
)

val location = if (dbUsername.isNotEmpty()) "mongodb://$dbUsername:$dbPassword@$dbHost:$dbPort" else "mongodb://$dbHost:$dbPort"

val allowedCrawlerHosts = listOf(
    Host(Url("https://www.britannica.com"),
        boostForUrls[0],
        PageRepository.MongoClient("ency", collectionName = "britannica", location = location),
        { url ->
            val split = url.cUrl().split("/")
            if (split.size >= 2) !split[1].contains("story")
            else true
        }),
    Host(Url("https://deletionpedia.org/en/Main_Page"),
        boostForUrls[1],
        PageRepository.MongoClient("ency", collectionName = "deletionpedia", location = location),
        { url -> true }),
    Host(Url("https://www.infoplease.com"),
        boostForUrls[2],
        PageRepository.MongoClient("ency", collectionName = "infoplease", location = location),
        { url -> true }),
//    Host(Url("https://www.encyclopedia.com"), 1.0, PageRepository.MongoClient("ency", collectionName = "encyclopedia"), { url -> true }),
    Host(Url("http://www.scholarpedia.org/article/Main_Page"),
        boostForUrls[3],
        PageRepository.MongoClient("ency", collectionName = "scholarpedia", location = location),
        { url -> true }),
    Host(Url("https://www.goodreads.com"),
        boostForUrls[4],
        PageRepository.MongoClient("ency", collectionName = "goodreads", location = location),
        { url ->
            val xdd = url.encodedPath.split("/")
            if (xdd.size >= 2) {
                val xd = xdd[1]
                xd == "book" || xd == "author" || xd == "genre"
            } else false
        }),
    Host(Url("https://en.wikipedia.org/wiki/Main_Page"),
        boostForUrls[5],
        PageRepository.MongoClient("ency", collectionName = "wikipedia", location = location),
        { url -> true }),
    Host(Url("https://ncatlab.org/nlab/show/HomePage"),
        boostForUrls[6],
        PageRepository.MongoClient("ency", collectionName = "ncatlab", location = location),
        { url -> true }),
)


class UrlDecider : UrlDecide() {
    private val currentlyCrawling = CurrentlyCrawling()

    override suspend fun get() = getUrl().let {
        currentlyCrawling.add(it.first)
        currentlyCrawling.addDomain(it.first)
        it
    }


    override suspend fun free(url: Url) {
        currentlyCrawling.remove(url)
        currentlyCrawling.removeDomain(url)
    }

    private suspend fun getUrl(): Pair<Url, Host> = try {
        randomPage()
    } catch (e: Exception) {
        println(e)
        getUrl()
    }

    private suspend fun randomPage(): Pair<Url, Host> {
        val host = allowedCrawlerHosts.map { host ->
            Pair(
                host, (host.crawledSoFar.toDouble() / host.boost)
            )
        }.minByOrNull { it.second }


        val dbClient = host?.first?.dbName ?: return randomPage()
        host.first.crawledSoFar += 1
        val page = dbClient.randomPages(1, 200).firstOrNull() ?: return randomPage()
        //if (currentlyCrawling.contains(Url(page.finalUrl))) {
        //    host.first.crawledSoFar -= 1
        //    return randomPage()
        //}
        val pageCont = page.content
        val suitableLinks = Jsoup.parse(pageCont).pageLinks(Url(page.finalUrl)).let { suitableUrls(it, host.first) }
        val suitableUnindexedLinks = suitableLinks.mapNotNull {
            if (isIndexed(it, host.first.dbName)) null else it
        }
        if (suitableUnindexedLinks.isEmpty()) {
            host.first.crawledSoFar -= 1
            return randomPage()
        }

//        if (suitableUnindexedLinks.isEmpty()) return randomPage(dbClient.find(suitableLinks.random().cUrl()).first())
        return Pair(suitableUnindexedLinks.random(), host.first)
    }

    private suspend fun isIndexed(link: Url, dbClient: PageRepository.Client): Boolean {
        return currentlyCrawling.contains(link) || currentlyCrawling.containsDomain(link) || dbClient.find(link.cUrl()) != null || dbClient.findTarget(
            link.cUrl()
        ).isNotEmpty()
    }

    private fun suitableUrls(urls: List<Url>, host: Host): List<Url> = urls.mapNotNull {
        if (it.host == host.host.host) {
            if (!host.fn(it)) return@mapNotNull null
            if (!it.parameters.isEmpty()) return@mapNotNull null
            if (it.fullPath.contains(":")) return@mapNotNull null
            if (it.fullPath.contains("quiz")) return@mapNotNull null
            return@mapNotNull it
        } else null
    }
}

private class CurrentlyCrawling {
    private val currentlyCrawling = mutableSetOf<String>()

    @OptIn(DelicateCoroutinesApi::class)
    private val context = newSingleThreadContext("CurrentlyCrawling")

    suspend fun add(url: Url) = withContext(context) { currentlyCrawling.add(url.cUrl()) }

    suspend fun remove(url: Url) = withContext(context) { currentlyCrawling.remove(url.cUrl()) }

    suspend fun contains(url: Url) = withContext(context) { currentlyCrawling.contains(url.cUrl()) }

    suspend fun addDomain(url: Url) = withContext(context) { currentlyCrawling.add(url.host) }

    suspend fun removeDomain(url: Url) = withContext(context) { currentlyCrawling.remove(url.host) }

    suspend fun containsDomain(url: Url) = withContext(context) { currentlyCrawling.contains(url.host) }

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

/*
@OptIn(ExperimentalCoroutinesApi::class, ExperimentalTime::class)
fun CoroutineScope.repositoryDocs(
    client: PageRepository.Client, DocBatchSize: Int = 200, limit: Int = Int.MAX_VALUE, includeBody: Boolean = true
): ReceiveChannel<PageRepository.Page> = produce(capacity = DocBatchSize + 1) {
    var lastUrl: String? = null
    var ctr = 0

    while (ctr < limit) {
        val value: TimedValue<Unit> = measureTimedValue {
            try {
                val finds: List<PageRepository.Page> = client.findAfter(lastUrl, DocBatchSize)
                if (finds.isEmpty()) return@produce
                finds.forEach { if (it.statusCode == 200) send(it) }
                lastUrl = finds.last().finalUrl
                ctr += finds.size
            } catch (e: Exception) {
                println("Error: $e")
                delay(10_000)
            }
        }
        println(
            "$ctr docs - ${(ctr.toDouble() / limit.toDouble() * 100.0).roundToInt()}%, took: ${value.duration.inWholeMinutes}min ${value.duration.inWholeSeconds % 60}s"
        )
    }

}
 */