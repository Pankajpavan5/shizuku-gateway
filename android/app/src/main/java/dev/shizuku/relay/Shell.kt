package dev.shizuku.relay

import rikka.shizuku.Shizuku
import java.lang.reflect.Method

// Runs a shell command as the shell user through Shizuku.
object Shell {
    private const val CAP = 8 * 1024 * 1024

    // shortcut: Shizuku.newProcess is compiled private in api 13.1.5, call it with
    // reflection; switch to Shizuku.bindUserService if a future version removes it.
    private val newProcess: Method = Shizuku::class.java
        .getDeclaredMethod(
            "newProcess",
            Array<String>::class.java,
            Array<String>::class.java,
            String::class.java,
        )
        .apply { isAccessible = true }

    data class Result(val code: Int, val out: String, val ok: Boolean)

    fun run(cmd: String, timeoutMs: Long = 60_000): Result {
        return try {
            val p = newProcess.invoke(null, arrayOf("sh", "-c", cmd), null, null) as Process
            val buf = arrayOf("", "")
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
                waiter.join(2000)
            }
            readers.forEach { it.join(3000) }
            if (waiter.isAlive) {
                return Result(-1, "Timeout after ${timeoutMs / 1000}s", false)
            }

            val out = (buf[0] + buf[1]).take(CAP)
            Result(code[0], out, code[0] == 0)
        } catch (t: Throwable) {
            Result(-1, t.message ?: t.javaClass.simpleName, false)
        }
    }

    private fun drain(stream: java.io.InputStream): String {
        val sb = StringBuilder()
        stream.bufferedReader().useLines { lines ->
            lines.forEach { line ->
                if (sb.length < CAP) sb.append(line).append('\n')
            }
        }
        return sb.toString()
    }
}
