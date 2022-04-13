import org.jsoup.nodes.Document

class IsWanted {

    private fun isDocInEnglish(doc: Document): Boolean {
        //        val correctPath = page.address.url.lowercase().contains("/en/")
        return langAttr(doc).lowercase().contains("en") || langAttr(doc) == "" // || correctPath
    }

    private fun langAttr(doc: Document): String = doc.attr("lang")

    private fun isNoIndex(doc: Document): Boolean {
        return doc.attr("robots").contains("noindex")
    }

    fun isWanted(doc: Document): Boolean {
        return listOf(
            isDocInEnglish(doc),
            !isNoIndex(doc),
        ).all { it }
    }
}