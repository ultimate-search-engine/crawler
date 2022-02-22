package HtmlParser

import libraries.*

import org.jsoup.Jsoup
import org.jsoup.nodes.Document


class MetadataByHTML(doc: Document) : Page.Metadata() {
    override val title: String = doc.title()
    override val description: String = doc.select("meta[name=description]").attr("content")
    override val openGraphImgURL: String = doc.select("meta[property=og:image]").attr("content")
    override val openGraphTitle: String = doc.select("meta[property=og:title]").attr("content")
    override val openGraphDescription: String = doc.select("meta[property=og:description]").attr("content")
    override val type: String = doc.select("meta[property=og:type]").attr("content")
    override val tags: List<String> = doc.select("meta[property=keywords]").map { it.attr("content") }.toList()
}

class HeadingsByHTML(doc: Document) : Page.Headings() {
    override val h1 = doc.select("h1").map { it.text() }.toList()
    override val h2 = doc.select("h2").map { it.text() }.toList()
    override val h3 = doc.select("h3").map { it.text() }.toList()
    override val h4 = doc.select("h4").map { it.text() }.toList()
    override val h5 = doc.select("h5").map { it.text() }.toList()
    override val h6 = doc.select("h6").map { it.text() }.toList()
}

class LinksByHTML(doc: Document, url: Url) : Page.BodyLinks() {
    override val internal = mutableListOf<Page.ForwardLink>()
    override val external = mutableListOf<Page.ForwardLink>()

    init {
        val links = doc.select("a[href]")
        for (link in links) {
            val href = cleanUrl(link.attr("href"))
            link.attr("href", href)
            if (href.startsWith("/")) {
                val baseUrl = url.cUrl.split("/").take(3).joinToString("/")

                link.attr("href", if (href.startsWith("//")) "" else "${baseUrl}$href")
            }
        }

        for (link in links.distinctBy { it.attr("href") }) {
            val href = link.attr("href")
            if (href == "" || !href.startsWith("https") || cleanUrl(href) == url.cUrl) continue
            if (getDomain(href).startsWith(getDomain(url.cUrl))) {
                internal.add(Page.ForwardLink(cleanText(link.text()), cleanUrl(href)))
            } else {
                external.add(Page.ForwardLink(cleanText(link.text()), cleanUrl(href)))
            }
        }
    }
}

class BodyByHTML(doc: Document, url: Url) : Page.Body() {
    override val headings = HeadingsByHTML(doc)
    override val boldText = doc.select("b").map { it.text() }.toList()
    override val article = listOf<String>() // doc.select("article").map { it.text() }.toList()
    override val links = LinksByHTML(doc, url)
}

class InferredDataByHTML(url: Url, override var backLinks: List<Page.BackLink>) : Page.InferredData() {
    override val ranks: Page.Ranks = Page.Ranks()
    override var domainName: String = getDomain(url.cUrl)
}

//                                  url is not cleaned
class HtmlParser(html: String, url: Url, backLinks: List<Page.BackLink>) : Page.PageType() {
    private val doc = Jsoup.parse(html)
    override val metadata = MetadataByHTML(doc)
    override val body = BodyByHTML(doc, url)

    override val address = Page.Address(url.cUrl, url.urlInWords)
    override var inferredData = InferredDataByHTML(url, backLinks)
    override var crawlerStatus: Page.CrawlerStatus = Page.CrawlerStatus.Crawled
    override val crawlerTimestamp: Long = System.currentTimeMillis()
}

class Url(val url: String) {
    val cUrl = cleanUrl(url)
    val urlInWords = splitUrlToWords(url)
    val domain = getDomain(cUrl)
    fun get(): String = url
}
