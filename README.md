# 24BDA70363-FULL-STACK-3.1.2

<!DOCTYPE html>
<html>
<head>
<title>Advanced Kafka Reliability</title>

<style>
body {
    font-family: Arial;
    background: #eef2f7;
    padding: 20px;
}

.container {
    max-width: 850px;
    margin: auto;
    background: white;
    padding: 25px;
    border-radius: 10px;
}

h2 {
    color: #1d4ed8;
}

input, select, button {
    padding: 10px;
    margin: 6px;
}

button {
    background: #2563eb;
    color: white;
    border: none;
    border-radius: 5px;
    cursor: pointer;
}

button:hover {
    background: #1e40af;
}

.metrics {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    margin: 15px 0;
}

.card {
    background: #dbeafe;
    padding: 15px;
    border-radius: 8px;
    min-width: 120px;
    flex: 1;
    text-align: center;
}

.log {
    background: #172033;
    color: #8fffa6;
    padding: 15px;
    min-height: 180px;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    border-radius: 5px;
}
</style>
</head>

<body>

<div class="container">

<h2>Advanced Kafka Reliability System</h2>
<p>Retry, Idempotency, Poison Pill and Bulk DLQ Replay</p>

<h3>Event Details</h3>

<input type="text" id="eventId"
placeholder="Enter Event ID">

<input type="text" id="post"
placeholder="Enter Post Content">

<select id="failure">
<option value="NONE">No Failure</option>
<option value="TRANSIENT">Transient Failure</option>
<option value="FATAL">Fatal Failure</option>
<option value="POISON">Poison Pill</option>
</select>

<br>

<button onclick="sendEvent()">Send Event</button>
<button onclick="sendDuplicate()">Test Duplicate</button>
<button onclick="sendBurst()">Concurrent Burst Demo</button>

<h3>Dead Letter Queue Management</h3>

<button onclick="showDLQ()">View DLQ</button>
<button onclick="replayAll()">Replay All Failed Events</button>

<h3>Reliability Metrics</h3>

<div class="metrics">

<div class="card">
<h3 id="processed">0</h3>
Processed
</div>

<div class="card">
<h3 id="duplicates">0</h3>
Duplicates
</div>

<div class="card">
<h3 id="retries">0</h3>
Retries
</div>

<div class="card">
<h3 id="failed">0</h3>
DLQ Messages
</div>

</div>

<p id="health">System Health: OPTIMAL</p>

<h3>Execution Output</h3>

<div class="log" id="output"></div>

<br>
<button onclick="clearLogs()">Clear Logs</button>

</div>

<script>

let processedEvents = new Set();
let dlq = [];
let processing = new Set();

let processedCount = 0;
let duplicateCount = 0;
let retryCount = 0;
let busy = false;

function log(message) {
    document.getElementById("output").textContent +=
        message + "\n";
}

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function updateMetrics() {
    document.getElementById("processed").innerText =
        processedCount;

    document.getElementById("duplicates").innerText =
        duplicateCount;

    document.getElementById("retries").innerText =
        retryCount;

    document.getElementById("failed").innerText =
        dlq.length;

    document.getElementById("health").innerText =
        "System Health: " +
        (dlq.length > 10 ? "DEGRADED" : "OPTIMAL");
}

async function processEvent(event) {

    if (processedEvents.has(event.id) ||
        processing.has(event.id)) {

        duplicateCount++;
        log("Duplicate event skipped: " + event.id);
        updateMetrics();
        return;
    }

    processing.add(event.id);

    try {

        if (event.failure === "POISON") {
            log("Poison Pill detected: " + event.id);
            dlq.push(event);
            log("Moved directly to DLQ.");
            return;
        }

        for (let attempt = 1; attempt <= 4; attempt++) {

            log("Processing " + event.id +
                " | Attempt " + attempt);

            if (event.failure === "NONE") {
                processedEvents.add(event.id);
                processedCount++;
                log("SUCCESS: " + event.post);
                return;
            }

            if (event.failure === "FATAL") {
                log("Fatal error. No retry.");
                break;
            }

            if (attempt < 4) {
                retryCount++;
                log("Transient failure. Exponential backoff...");
                await delay(100 * Math.pow(1.5, attempt - 1));
            }
        }

        dlq.push(event);
        log("Event moved to DLQ: " + event.id);

    } finally {
        processing.delete(event.id);
        updateMetrics();
    }
}

function readEvent() {

    const id = document.getElementById("eventId").value.trim();
    const post = document.getElementById("post").value.trim();
    const failure = document.getElementById("failure").value;

    if (!id || !post) {
        alert("Enter Event ID and Post Content");
        return null;
    }

    return { id, post, failure };
}

async function sendEvent() {
    if (busy) return;

    const event = readEvent();
    if (!event) return;

    busy = true;
    try {
        await processEvent(event);
    } finally {
        busy = false;
    }
}

async function sendDuplicate() {
    if (busy) return;

    const event = readEvent();
    if (!event) return;

    busy = true;

    try {
        log("Sending same event twice...");

        await processEvent(event);
        await processEvent(event);
    } finally {
        busy = false;
    }
}

async function sendBurst() {
    if (busy) return;

    const event = readEvent();
    if (!event) return;

    busy = true;

    try {
        log("Simulating concurrent duplicate burst...");

        await Promise.all(
            Array.from({ length: 6 }, () =>
                processEvent({ ...event })
            )
        );
    } finally {
        busy = false;
    }
}

function showDLQ() {

    log("----- DLQ MESSAGES -----");

    if (dlq.length === 0) {
        log("Dead Letter Queue is empty.");
        return;
    }

    dlq.forEach((event, index) => {
        log((index + 1) + ". " +
            event.id + " - " + event.failure);
    });
}

async function replayAll() {
    if (busy) return;

    if (dlq.length === 0) {
        log("No messages available for replay.");
        return;
    }

    busy = true;

    const failedEvents = [...dlq];
    dlq = [];
    updateMetrics();

    try {
        log("Starting Bulk DLQ Replay...");

        for (const event of failedEvents) {

            event.failure = "NONE";

            log("Replaying: " + event.id);

            await processEvent(event);
        }

        log("Bulk replay completed.");

    } finally {
        busy = false;
        updateMetrics();
    }
}

function clearLogs() {
    document.getElementById("output").textContent = "";
}

updateMetrics();

</script>

</body>
</html>
