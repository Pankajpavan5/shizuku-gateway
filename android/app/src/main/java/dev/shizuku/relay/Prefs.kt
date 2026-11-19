package dev.shizuku.relay

import android.content.Context

object Prefs {
    private const val FILE = "relay"

    fun url(c: Context): String =
        c.getSharedPreferences(FILE, 0).getString("url", "") ?: ""

    fun token(c: Context): String =
        c.getSharedPreferences(FILE, 0).getString("token", "") ?: ""

    fun ask(c: Context): Boolean =
        c.getSharedPreferences(FILE, 0).getBoolean("ask", true)

    fun ping(c: Context): Int =
        (c.getSharedPreferences(FILE, 0).getInt("ping", 30)).coerceIn(1, 120)

    fun save(c: Context, url: String, token: String, ask: Boolean, ping: Int) {
        c.getSharedPreferences(FILE, 0).edit()
            .putString("url", url)
            .putString("token", token)
            .putBoolean("ask", ask)
            .putInt("ping", ping.coerceIn(1, 120))
            .apply()
    }
}
