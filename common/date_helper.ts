export let TIMEZONE_OFFSET = 8; // GMT-08:00. Only works with negative timezone offset.
export let TIMEZONE_OFFSET_STRING = TIMEZONE_OFFSET.toString().padStart(2, "0");

export function toDateISOString(date: Date): string {
  let year = date.getUTCFullYear().toString().padStart(4, "0");
  let month = (date.getUTCMonth() + 1).toString().padStart(2, "0");
  let day = date.getUTCDate().toString().padStart(2, "0");
  return `${year}-${month}-${day}`;
}

export function toMonthISOString(date: Date): string {
  let year = date.getUTCFullYear().toString().padStart(4, "0");
  let month = (date.getUTCMonth() + 1).toString().padStart(2, "0");
  return `${year}-${month}`;
}

// Coverted to today's date wrt. the timezone. The following calls should always use UTC*() functions. `date` is changed in place.
export function toToday(date: Date): Date {
  if (date.getUTCHours() < TIMEZONE_OFFSET) {
    date.setUTCDate(date.getUTCDate() - 1);
  }
  return date;
}

// Convert to yesterday's date wrt. the timezone. `date` is changed in place.
export function toYesterday(date: Date): Date {
  toToday(date);
  date.setUTCDate(date.getUTCDate() - 1);
  return date;
}

// Returns the timestamp of the start of the month wrt. the timezone.
export function toMonthTimeMsWrtTimezone(monthISOString: string): number {
  return new Date(
    `${monthISOString}-01T${TIMEZONE_OFFSET_STRING}:00Z`,
  ).valueOf();
}
