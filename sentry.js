// Error monitoring for the socket server. Off unless SENTRY_DSN is set (Render → Environment).
// No personal data: no IPs, no request bodies, no auth headers / tokens.
import * as Sentry from "@sentry/node"

export const sentryOn = !!process.env.SENTRY_DSN

if (sentryOn) {
  Sentry.init({
    dsn: process.env.SENTRY_DSN,
    environment: process.env.SENTRY_ENV || process.env.NODE_ENV || "production",
    sendDefaultPii: false,
    tracesSampleRate: 0,
    beforeSend(event) {
      if (event.request) {
        delete event.request.cookies
        delete event.request.data
        if (event.request.headers) for (const k of Object.keys(event.request.headers)) if (/cookie|authorization|x-emit-secret|token/i.test(k)) event.request.headers[k] = "[Filtered]"
      }
      if (event.user) event.user = event.user.id ? { id: event.user.id } : undefined
      return event
    },
  })
}

/** Report an error (no-op when Sentry is off). */
export function report(err, context) {
  if (!sentryOn) return
  Sentry.captureException(err, context ? { extra: context } : undefined)
}

export { Sentry }
