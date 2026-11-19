package dev.shizuku.relay

import rikka.shizuku.Shizuku
import java.lang.reflect.Method

// Runs a shell command as the shell user through Shizuku.
object Shell {
    private const val CAP = 8 * 1024 * 1024
    private var method: Method? = null

    // shortcut: Shizuku.newProcess is compiled private in api 13.1.5, call it with
    // reflection; switch to Shizuku.bindUserService if a future version removes it.
    private fun newProcess(): Method = method ?: Shizuku::class.java
        .getDeclaredMethod(
            "newProcess",
            Array<String>::class.java,
            Array<String>::class.java,
            String::class.java,
        )
        .apply { isAccessible = true }
        .also { method = it }

    data class Result(val code: Int, val out: String, val ok: Boolean)

    fun run(cmd: String, timeoutMs: Long = 30_000): Result {
        return try {
            val p = newProcess().invoke(null, arrayOf("sh", "-c", cmd), null, null) as Process
            val buf = arrayOf<String?>(null, null)
            val readers = listOf(
                Thread { buf[0] = drain(p.inputStream) },
                Thread { buf[1] = drain(p.errorStream) },
            )
            readers.forEach { it.start() }

            // ShizukuRemoteProcess has no waitFor(timeout): block in a thread instead.
            val code = intArrayOf(-1)
            val waiter = Thread {
                try {
                    code[0] = p.waitFor()
                } catch (_: Throwable) {
                }
            }
            waiter.start()
            waiter.join(timeoutMs)
            if (waiter.isAlive) {
                p.destroy()
                waiter.join(1000)
                readers.forEach { it.join(1000) }
                // No read of buf here: the readers may still write it.
                return Result(-1, "Timeout after ${timeoutMs / 1000}s", false)
            }
            readers.forEach { it.join(3000) }
            if (readers.any { it.isAlive }) return Result(-1, "Output read did not finish", false)

            val outA = buf[0]
            val outB = buf[1]
            if (outA == null || outB == null) {
                return Result(-1, "Output exceeds ${CAP / 1024 / 1024} MB", false)
            }
            Result(code[0], outA + outB, code[0] == 0)
        } catch (t: Throwable) {
            Result(-1, t.message ?: t.javaClass.simpleName, false)
        }
    }

    // null means the output hit CAP; the stream is still drained.
    private fun drain(stream: java.io.InputStream): String? {
        val sb = StringBuilder()
        var cut = false
        stream.bufferedReader().useLines { lines ->
            lines.forEach { line ->
                if (sb.length + line.length + 1 > CAP) {
                    cut = true
                } else {
                    sb.append(line).append('\n')
                }
            }
        }
        return if (cut) null else sb.toString()
    }
}
