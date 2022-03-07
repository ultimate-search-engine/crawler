import libraries.Page
import org.jsoup.nodes.Document

class IsWanted(private val doc: Document, private val page: Page.PageType) {

    private fun isDocInEnglish(): Boolean {
        val correctLang = langAttr().lowercase().contains("en") || langAttr().contains("")
        val correctPath = page.address.url.lowercase().contains("/en/")
        return correctLang || correctPath
    }

    private fun langAttr(): String = doc.attr("lang")

    private fun isNoIndex(): Boolean {
        return doc.attr("robots").contains("noindex")
    }

    fun isWanted(): Boolean {
        return listOf(
            isDocInEnglish(),
            !isNoIndex(),
        ).all { it }
    }
}