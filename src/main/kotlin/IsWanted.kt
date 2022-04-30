import io.ktor.http.*
import org.jsoup.nodes.Document
import java.io.File

class IsWanted {
    private val inferLanguage = InferLanguage()

    private fun englishTag(doc: Document): Boolean = langAttr(doc).lowercase().contains("en")

    private fun langAttr(doc: Document): String = doc.attr("lang")

    private fun isNoIndex(doc: Document): Boolean = doc.attr("robots").contains("noindex")

    fun isWanted(doc: Document): Boolean {
        return listOf(
            if (englishTag(doc)) true else inferLanguage.isEnglish(doc) > 0.54,
            !isNoIndex(doc),
        ).all { it }
    }
}


class InferLanguage {
    private val minimalWordLength = 3

    private val allWordTokens = File("./libraries/corpus.txt").readLines().mapNotNull {
        if (it.startsWith("#")) null
        else if (it.length < minimalWordLength) null
        else it.lowercase()
    }.sorted()

    private fun getWords(doc: Document): List<String> {
        return doc.select("body").text().split(" ", ".", ",", "'", "\"", ":", ";").filter { it.count() >= minimalWordLength && !it.matches(Regex("[1-9]+")) }
    }

    fun isEnglish(doc: Document): Double {
        val words = getWords(doc)
        val totalWords = words.count()
        var totalEnglish = 0

        for (word in words) {
            // regex if every character is a letter
            if (word.matches(Regex("[a-zA-Z]+"))) {
                val wordIndex = allWordTokens.binarySearch(word.lowercase())
                if (wordIndex >= 0 && wordIndex < allWordTokens.size) totalEnglish++
            }
        }
//        println("$totalEnglish / $totalWords = ${totalEnglish.toDouble() / totalWords} - ${url.cUrl()}")
        return totalEnglish.toDouble() / totalWords.coerceAtLeast(1)
    }

}
