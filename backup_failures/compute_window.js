// compute_window (ET window starts daily at 18:00 America/New_York)
//
// Outputs (use these in workflow):
// - correlationId : "Cohesity_Backup_Failures" (constant)
// - windowKey     : "YYYY-MM-DD_1800ET"       (e.g., 2025-12-18_1800ET)
// - windowLabel   : "YYYY-MM-DD 18:00 ET → YYYY-MM-DD 18:00 ET"
// - snStartUtc    : "YYYY-MM-DD HH:MM:SS"     (UTC, for sys_created_on compare)
// - snEndUtc      : "YYYY-MM-DD HH:MM:SS"     (UTC, for sys_created_on compare)

export default async function () {
  const TZ = "America/New_York";
  const START_HOUR = 18;
  const correlationId = "Cohesity_Backup_Failures";

  const pad2 = (n) => String(n).padStart(2, "0");

  // Get parts of a Date as seen in a specific timezone (DST-aware)
  function tzParts(date, timeZone) {
    const dtf = new Intl.DateTimeFormat("en-US", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false
    });
    const parts = dtf.formatToParts(date);
    const get = (t) => (parts.find((p) => p.type === t) || {}).value;
    return {
      y: Number(get("year")),
      m: Number(get("month")),
      d: Number(get("day")),
      h: Number(get("hour")),
      min: Number(get("minute")),
      s: Number(get("second"))
    };
  }

  const ymd = (y, m, d) => `${y}-${pad2(m)}-${pad2(d)}`;

  // DST-safe addDays for YYYY-MM-DD using UTC noon anchor
  function addDays(ymdStr, days) {
    const [yy, mm, dd] = ymdStr.split("-").map(Number);
    const dt = new Date(Date.UTC(yy, mm - 1, dd, 12, 0, 0));
    dt.setUTCDate(dt.getUTCDate() + days);
    return ymd(dt.getUTCFullYear(), dt.getUTCMonth() + 1, dt.getUTCDate());
  }

  // Timezone offset helper (ms): tzWallTime(UTC instant) - UTC
  function getTzOffsetMs(dateUtcInstant, timeZone) {
    const p = tzParts(dateUtcInstant, timeZone);
    const asUTC = Date.UTC(p.y, p.m - 1, p.d, p.h, p.min, p.s);
    return asUTC - dateUtcInstant.getTime();
  }

  // Convert wall time in TZ -> UTC Date (DST-aware; 2-pass to stabilize around DST)
  function zonedWallToUtcDate(y, m, d, hh, mm, ss, timeZone) {
    // initial guess: treat wall time as UTC
    let guess = new Date(Date.UTC(y, m - 1, d, hh, mm, ss));
    // adjust by offset at guess
    let offset = getTzOffsetMs(guess, timeZone);
    let utc = new Date(guess.getTime() - offset);

    // second pass (DST transitions)
    offset = getTzOffsetMs(utc, timeZone);
    utc = new Date(guess.getTime() - offset);

    return utc;
  }

  function formatUtcForSnow(dateUtc) {
    // UTC "YYYY-MM-DD HH:MM:SS"
    return (
      dateUtc.getUTCFullYear() +
      "-" + pad2(dateUtc.getUTCMonth() + 1) +
      "-" + pad2(dateUtc.getUTCDate()) +
      " " + pad2(dateUtc.getUTCHours()) +
      ":" + pad2(dateUtc.getUTCMinutes()) +
      ":" + pad2(dateUtc.getUTCSeconds())
    );
  }

  // -----------------------------
  // Decide current ET window start date (18:00 ET boundary)
  // -----------------------------
  const now = new Date();
  const nowEt = tzParts(now, TZ);

  const todayEtStr = ymd(nowEt.y, nowEt.m, nowEt.d);

  // If before 18:00 ET, current window started yesterday at 18:00 ET
  const startDateStr = (nowEt.h < START_HOUR) ? addDays(todayEtStr, -1) : todayEtStr;
  const endDateStr   = addDays(startDateStr, 1);

  const windowKey   = `${startDateStr}_1800ET`;
  const windowLabel = `${startDateStr} 18:00 ET → ${endDateStr} 18:00 ET`;

  // -----------------------------
  // Build UTC boundaries for ServiceNow sys_created_on comparisons
  // (sys_created_on is usually UTC-formatted "YYYY-MM-DD HH:MM:SS")
  // -----------------------------
  const [sy, sm, sd] = startDateStr.split("-").map(Number);
  const [ey, em, ed] = endDateStr.split("-").map(Number);

  const startUtcDate = zonedWallToUtcDate(sy, sm, sd, 18, 0, 0, TZ);
  const endUtcDate   = zonedWallToUtcDate(ey, em, ed, 18, 0, 0, TZ);

  const snStartUtc = formatUtcForSnow(startUtcDate);
  const snEndUtc   = formatUtcForSnow(endUtcDate);

  return {
    correlationId,
    windowKey,
    windowLabel,
    snStartUtc,
    snEndUtc
  };
}
