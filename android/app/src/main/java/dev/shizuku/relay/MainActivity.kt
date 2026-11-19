package dev.shizuku.relay

import android.Manifest
import android.app.Activity
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.os.Build
import android.os.Bundle
import android.text.InputType
import android.view.Gravity
import android.view.ViewGroup
import android.view.WindowManager
import android.widget.Button
import android.widget.CheckBox
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import androidx.core.content.ContextCompat
import rikka.shizuku.Shizuku

class MainActivity : Activity() {

    private lateinit var urlField: EditText
    private lateinit var tokenField: EditText
    private lateinit var pingField: EditText
    private lateinit var askBox: CheckBox
    private lateinit var state: TextView

    private val permissionListener =
        Shizuku.OnRequestPermissionResultListener { _, grant ->
            if (grant == PackageManager.PERMISSION_GRANTED) launchRelay() else setState("Shizuku permission denied")
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.setFlags(WindowManager.LayoutParams.FLAG_SECURE, WindowManager.LayoutParams.FLAG_SECURE)
        buildUi()

        val (u, t, p) = Triple(Prefs.url(this), Prefs.token(this), Prefs.ping(this))
        urlField.setText(u)
        tokenField.setText(t)
        pingField.setText(p.toString())
        askBox.isChecked = Prefs.ask(this)
        setState(RelayService.status)

        if (Build.VERSION.SDK_INT >= 33 &&
            checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }
        Shizuku.addRequestPermissionResultListener(permissionListener)
    }

    override fun onResume() {
        super.onResume()
        RelayService.onStatus = { setState(it) }
        setState(RelayService.status)
    }

    override fun onPause() {
        RelayService.onStatus = null
        super.onPause()
    }

    override fun onDestroy() {
        Shizuku.removeRequestPermissionResultListener(permissionListener)
        super.onDestroy()
    }

    private fun launchRelay() {
        ContextCompat.startForegroundService(this, Intent(this, RelayService::class.java))
        setState("Relay started")
    }

    private fun startClicked() {
        val url = urlField.text.toString().trim()
        val token = tokenField.text.toString().trim()
        if (!url.startsWith("wss://")) {
            setState("URL must start with wss://")
            return
        }
        if (token.length < 16) {
            setState("Relay token is too short")
            return
        }
        val ping = pingField.text.toString().toIntOrNull()?.coerceIn(1, 120) ?: 30
        Prefs.save(this, url, token, askBox.isChecked, ping)

        if (!Shizuku.pingBinder()) {
            setState("Shizuku is not running. Start the Shizuku app first.")
            return
        }
        if (Shizuku.isPreV11()) {
            setState("Shizuku 11 or newer is required.")
            return
        }
        if (Shizuku.checkSelfPermission() == PackageManager.PERMISSION_GRANTED) {
            launchRelay()
            return
        }
        if (Shizuku.shouldShowRequestPermissionRationale()) {
            setState("Grant Shizuku permission in the Shizuku app.")
            return
        }
        setState("Waiting for Shizuku permission")
        Shizuku.requestPermission(0)
    }

    private fun stopClicked() {
        stopService(Intent(this, RelayService::class.java))
        setState("Relay stopped")
    }

    private fun setState(s: String) {
        state.text = s
    }

    private fun buildUi() {
        val pad = (24 * resources.displayMetrics.density).toInt()
        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(pad, pad, pad, pad)
        }

        root.addView(title("Shizuku Relay"))
        root.addView(note("This app runs shell commands that an MCP client sends. Keep the token secret."))

        urlField = field("wss://your-service.onrender.com/relay", InputType.TYPE_CLASS_TEXT)
        tokenField = field("Relay token", InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_VISIBLE_PASSWORD)
        pingField = field("Heartbeat seconds (1-120)", InputType.TYPE_CLASS_NUMBER)
        pingField.setText("30")
        askBox = CheckBox(this).apply {
            text = "Ask before each command"
            isChecked = true
        }

        root.addView(label("Relay URL"))
        root.addView(urlField)
        root.addView(label("Relay token"))
        root.addView(tokenField)
        root.addView(label("Heartbeat"))
        root.addView(pingField)
        root.addView(askBox)

        val row = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL }
        val start = Button(this).apply { text = "Start" }
        start.setOnClickListener { startClicked() }
        val stop = Button(this).apply { text = "Stop" }
        stop.setOnClickListener { stopClicked() }
        row.addView(start, LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f))
        row.addView(stop, LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f))
        root.addView(row)

        state = TextView(this).apply {
            textSize = 15f
            setTextColor(Color.DKGRAY)
            setPadding(0, pad / 2, 0, 0)
        }
        root.addView(state)
        root.addView(note("The relay works only while this service runs. Android shows a notification. The service stops after 30 idle minutes."))
        root.addView(note("Shizuku must be installed and running. This app does not use root."))

        setContentView(
            ScrollView(this).apply { addView(root) },
        )
    }

    private fun title(s: String) = TextView(this).apply {
        text = s
        textSize = 22f
        setPadding(0, 0, 0, 12)
    }

    private fun label(s: String) = TextView(this).apply {
        text = s
        textSize = 13f
        setPadding(0, 18, 0, 4)
    }

    private fun note(s: String) = TextView(this).apply {
        text = s
        textSize = 13f
        gravity = Gravity.START
        setTextColor(Color.GRAY)
        setPadding(0, 10, 0, 0)
    }

    private fun field(hint: String, type: Int) = EditText(this).apply {
        this.hint = hint
        inputType = type
        setSingleLine(true)
    }
}
