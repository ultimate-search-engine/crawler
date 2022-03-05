import htmlParser.HtmlParser
import libraries.Page
import org.jsoup.nodes.Document

class Language(private val doc: Document, private val page: Page.PageType) {
    enum class SupportedLanguages {
        En,
//        Cs,
    }

    fun isDocInEnglish(): Boolean {
        val correctLang = langAttr().lowercase().contains("en") || langAttr().contains("")
        val correctPath = page.address.url.contains("/en/")
        return correctLang || correctPath
    }

    private fun langAttr(): String = doc.attr("lang")

}