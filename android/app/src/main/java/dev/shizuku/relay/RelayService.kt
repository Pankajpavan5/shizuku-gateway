package dev.shizuku.relay

import android.app.KeyguardManager
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.SystemClock
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class RelayService : Service() {

    companion object {
        const val ACTION_STOP = "dev.shizuku.relay.STOP"
        const val ACTION_APPROVE = "dev.shizuku.relay.APPROVE"
        const val ACTION_DENY = "dev.shizuku.relay.DENY"

        private const val CHANNEL = "relay"
        private const val ASK_CHANNEL = "ask"
        private const val NOTIF_ID = 1
        private const val ASK_ID = 2
        private const val IDLE_MS = 30 * 60 * 1000L
        private const val APPROVE_PERMISSION = "dev.shizuku.relay.APPROVE"

        @Volatile
        var status: String = "stopped"
        var onStatus: ((String) -> Unit)? = null
    }

    private val client by lazy {
        OkHttpClient.Builder()
            .pingInterval(Prefs.ping(this).toLong(), TimeUnit.SECONDS)
            .build()
    }
    private val work = Executors.newSingleThreadExecutor()
    private val main = Handler(Looper.getMainLooper())
    @Volatile
    private var socket: WebSocket? = null
    private var retry = 0

    @Volatile
    private var stopped = false

    @Volatile
    private var lastCmdAt = SystemClock.elapsedRealtime()

    private val busy = AtomicBoolean(false)

    private val approvalLock = java.lang.Object()
    private var approvalId = -1L
    private var approvalAnswer: Boolean? = null
    private var receiverOn = false

    override fun onCreate() {
        super.onCreate()
        createChannels()
        startForegroundCompat(notice("Starting"))
        registerForApprovals()
        main.postDelayed(idleCheck, 60_000)
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) {
            stopped = true
            stopSelf()
            return START_NOT_STICKY
        }
        if (socket == null) connect()
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onDestroy() {
        stopped = true
        socket?.cancel()
        socket = null
        main.removeCallbacksAndMessages(null)
        if (receiverOn) {
            runCatching { unregisterReceiver(approvals) }
        }
        work.shutdown()
        setStatus("stopped")
        super.onDestroy()
    }

    // ---------- link ----------

    private fun connect() {
        if (stopped || socket != null) return
        val url = Prefs.url(this)
        val token = Prefs.token(this)
        if (url.isEmpty() || token.isEmpty()) {
            setStatus("Set the relay URL and token")
            return
        }
        setStatus("Connecting")
        val req = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $token")
            .build()
        socket = client.newWebSocket(req, listener)
    }

    private val listener = object : WebSocketListener() {
        override fun onOpen(ws: WebSocket, response: Response) {
            if (ws !== socket) {
                ws.close(1000, null)
                return
            }
            retry = 0
            ws.send(
                JSONObject().put(
                    "hello",
                    JSONObject()
                        .put("model", Build.MODEL)
                        .put("android", Build.VERSION.RELEASE)
                        .put("sdk", Build.VERSION.SDK_INT),
                ).toString(),
            )
            setStatus("Connected")
        }

        override fun onMessage(ws: WebSocket, text: String) {
            if (ws !== socket) return
            val o = runCatching { JSONObject(text) }.getOrNull() ?: return
            val id = o.optLong("id", -1)
            val cmd = o.optString("cmd")
            if (id < 0 || cmd.isEmpty()) return
            lastCmdAt = SystemClock.elapsedRealtime()
            work.execute { handle(ws, id, cmd) }
        }

        override fun onFailure(ws: WebSocket, t: Throwable, response: Response?) {
            if (ws !== socket) return
            socket = null
            setStatus("Link lost: ${t.message ?: t.javaClass.simpleName}")
            reconnect()
        }

        override fun onClosed(ws: WebSocket, code: Int, reason: String) {
            if (ws !== socket) return
            socket = null
            setStatus("Link closed")
            reconnect()
        }
    }

    private fun reconnect() {
        if (stopped) return
        val delay = minOf(60_000L, 2_000L shl minOf(retry, 5))
        retry++
        main.removeCallbacks(reconnectTask)
        main.postDelayed(reconnectTask, delay)
    }

    private val reconnectTask = Runnable { connect() }

    // ---------- command ----------

    private fun handle(ws: WebSocket, id: Long, cmd: String) {
        // One at a time. A late command would otherwise queue behind a run that
        // the relay already timed out.
        if (!busy.compareAndSet(false, true)) {
            reply(ws, id, false, "Busy. Try again.", -1)
            return
        }
        try {
            if (cmd.length > 4096) {
                reply(ws, id, false, "Command too long.", -1)
                return
            }
            if (Prefs.ask(this)) {
                if (cmd.length > 800) {
                    reply(ws, id, false, "Command too long for approval (max 800 chars).", -1)
                    return
                }
                if (getSystemService(KeyguardManager::class.java)?.isKeyguardLocked == true) {
                    reply(ws, id, false, "Phone is locked. Unlock it and retry.", -1)
                    return
                }
                if (!NotificationManagerCompat.from(this).areNotificationsEnabled()) {
                    reply(ws, id, false, "Notifications are off. Allow them for this app.", -1)
                    return
                }
                if (!askUser(id, cmd)) {
                    reply(ws, id, false, "Denied.", -1)
                    return
                }
            }
            val r = Shell.run(cmd)
            reply(ws, id, r.ok, r.out, r.code)
        } finally {
            busy.set(false)
        }
    }

    private fun reply(ws: WebSocket, id: Long, ok: Boolean, out: String, code: Int) {
        ws.send(
            JSONObject()
                .put("id", id)
                .put("ok", ok)
                .put("out", out)
                .put("code", code)
                .toString(),
        )
    }

    private fun askUser(id: Long, cmd: String): Boolean {
        synchronized(approvalLock) {
            approvalId = id
            approvalAnswer = null
        }
        val n = NotificationCompat.Builder(this, ASK_CHANNEL)
            .setSmallIcon(android.R.drawable.ic_dialog_alert)
            .setContentTitle("Run this command?")
            .setContentText(cmd.take(200))
            .setStyle(NotificationCompat.BigTextStyle().bigText(cmd.take(800)))
            .setPriority(NotificationCompat.PRIORITY_HIGH)
            .setVisibility(NotificationCompat.VISIBILITY_SECRET)
            .setAutoCancel(true)
            .setTimeoutAfter(30_000)
            .addAction(0, "Deny", approvalIntent(ACTION_DENY, id, 3))
            .addAction(0, "Approve", approvalIntent(ACTION_APPROVE, id, 4))
            .build()
        runCatching { NotificationManagerCompat.from(this).notify(ASK_ID, n) }

        val deadline = System.currentTimeMillis() + 30_000
        synchronized(approvalLock) {
            while (approvalAnswer == null && !stopped) {
                val left = deadline - System.currentTimeMillis()
                if (left <= 0) break
                approvalLock.wait(left)
            }
            val ok = approvalAnswer == true && approvalId == id
            approvalAnswer = null
            runCatching { NotificationManagerCompat.from(this).cancel(ASK_ID) }
            return ok
        }
    }

    private fun approvalIntent(action: String, id: Long, code: Int): PendingIntent =
        PendingIntent.getBroadcast(
            this,
            code,
            Intent(action).setPackage(packageName).putExtra("id", id),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )

    private fun registerForApprovals() {
        val filter = IntentFilter().apply {
            addAction(ACTION_APPROVE)
            addAction(ACTION_DENY)
        }
        if (Build.VERSION.SDK_INT >= 33) {
            ContextCompat.registerReceiver(this, approvals, filter, ContextCompat.RECEIVER_NOT_EXPORTED)
        } else {
            // API 26-32: a runtime receiver is exported by default.
            registerReceiver(approvals, filter, APPROVE_PERMISSION, null)
        }
        receiverOn = true
    }

    private val approvals = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            val id = intent?.getLongExtra("id", -1) ?: return
            synchronized(approvalLock) {
                if (id != approvalId) return
                approvalAnswer = intent.action == ACTION_APPROVE
                approvalLock.notifyAll()
            }
        }
    }

    // ---------- lifecycle ----------

    private val idleCheck = object : Runnable {
        override fun run() {
            if (SystemClock.elapsedRealtime() - lastCmdAt > IDLE_MS) {
                setStatus("Idle for 30 min. Stopped")
                stopped = true
                stopSelf()
                return
            }
            main.postDelayed(this, 60_000)
        }
    }

    private fun setStatus(s: String) {
        status = s
        main.post {
            onStatus?.invoke(s)
            if (!stopped) runCatching { NotificationManagerCompat.from(this).notify(NOTIF_ID, notice(s)) }
        }
    }

    private fun startForegroundCompat(n: Notification) {
        if (Build.VERSION.SDK_INT >= 34) {
            startForeground(NOTIF_ID, n, ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE)
        } else {
            startForeground(NOTIF_ID, n)
        }
    }

    private fun notice(text: String): Notification =
        NotificationCompat.Builder(this, CHANNEL)
            .setSmallIcon(android.R.drawable.stat_sys_upload)
            .setContentTitle("Shizuku Relay")
            .setContentText(text)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setContentIntent(
                PendingIntent.getActivity(
                    this,
                    0,
                    Intent(this, MainActivity::class.java),
                    PendingIntent.FLAG_IMMUTABLE,
                ),
            )
            .addAction(
                0,
                "Stop",
                PendingIntent.getService(
                    this,
                    1,
                    Intent(this, RelayService::class.java).setAction(ACTION_STOP),
                    PendingIntent.FLAG_IMMUTABLE,
                ),
            )
            .build()

    private fun createChannels() {
        val nm = getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(
            NotificationChannel(CHANNEL, "Relay", NotificationManager.IMPORTANCE_LOW),
        )
        nm.createNotificationChannel(
            NotificationChannel(ASK_CHANNEL, "Command approval", NotificationManager.IMPORTANCE_HIGH),
        )
    }
}
