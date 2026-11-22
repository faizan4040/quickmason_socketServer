/* Job scheduler for the Next.js cron routes.
   Vercel Hobby only runs crons once a day, but payments / refunds / releases need minutes.
   This always-on server calls the routes on time with "Authorization: Bearer CRON_SECRET".
   The routes themselves hold a MongoDB lock (lib/cronLock.js) and are idempotent, so a second
   scheduler instance, a Vercel backup cron or a retry can never double-run money work.

   Env:
     APP_URL            https://your-site (the Next.js app; NO trailing slash)
     CRON_SECRET        same value as on Vercel
     SCHEDULER_ENABLED  "false" to switch off (default on when APP_URL + CRON_SECRET exist) */

const JOBS = [
  { path: "/api/cron/payments", everyMs: 5 * 60_000 },            // checkouts, refunds, accept timeouts, return refunds
  { path: "/api/cron/release-settlements", everyMs: 60 * 60_000 }, // escrow release + repair; recon/invoices once a day inside
  { path: "/api/cron/cod-dues", everyMs: 6 * 60 * 60_000 },        // runs once per IST day inside
  { path: "/api/cron/product-stats", everyMs: 30 * 60_000 },
];
const TIMEOUT_MS = 70_000 // a bit more than the routes' maxDuration (60 s)

export function startScheduler({ log = console.log } = {}) {
  const base = String(process.env.APP_URL || "").replace(/\/+$/, "")
  const secret = process.env.CRON_SECRET
  if (process.env.SCHEDULER_ENABLED === "false" || !base || !secret) {
    log("scheduler: off", !base ? "(APP_URL missing)" : !secret ? "(CRON_SECRET missing)" : "")
    return { stop() {}, status: () => ({ enabled: false }) }
  }
  const state = Object.fromEntries(JOBS.map((j) => [j.path, { running: false, lastRunAt: null, lastStatus: null, lastMs: null, failures: 0 }]))
  const timers = []

  async function run(job) {
    const s = state[job.path]
    if (s.running) return // previous call still going: skip this tick
    s.running = true
    const t = Date.now()
    const ctrl = new AbortController()
    const kill = setTimeout(() => ctrl.abort(), TIMEOUT_MS)
    try {
      const res = await fetch(`${base}${job.path}`, { headers: { Authorization: `Bearer ${secret}` }, signal: ctrl.signal })
      s.lastStatus = res.status
      s.failures = res.ok ? 0 : s.failures + 1
      if (!res.ok) log(`scheduler: ${job.path} → HTTP ${res.status}`)
    } catch (e) {
      s.lastStatus = e.name === "AbortError" ? "timeout" : "error"
      s.failures += 1
      log(`scheduler: ${job.path} failed: ${e.message}`)
    } finally {
      clearTimeout(kill)
      s.running = false
      s.lastRunAt = new Date()
      s.lastMs = Date.now() - t
    }
  }

  JOBS.forEach((job, i) => {
    // stagger the first runs so a restart doesn't fire everything at once
    timers.push(setTimeout(() => {
      run(job)
      timers.push(setInterval(() => run(job), job.everyMs))
    }, 15_000 + i * 20_000 + Math.floor(Math.random() * 5_000)))
  })
  log(`scheduler: on → ${base} (${JOBS.length} jobs)`)
  return {
    stop: () => timers.forEach((t) => (clearTimeout(t), clearInterval(t))),
    status: () => ({ enabled: true, jobs: state }),
  }
}
