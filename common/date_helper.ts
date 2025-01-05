import { TIMEZONE_OFFSET } from "./params";

export let TIMEZONE_OFFSET_STRING = TIMEZONE_OFFSET.toString().padStart(2, "0");

export function toDateISOString(date: Date): string {
  let year = date.getUTCFullYear().toString().padStart(4, "0");
  let month = (date.getUTCMonth() + 1).toString().padStart(2, "0");
  let day = date.getUTCDate().toString().padStart(2, "0");
  return `${year}-${month}-${day}`;
}

export function toTommorrowISOStringFromString(dateISOString: string): string {
  let date = new Date(`${dateISOString}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + 1);
  return toDateISOString(date);
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

export function toDateUtc(dateISOString: string): Date {
  return new Date(`${dateISOString}T00:00Z`);
}

// Returns the timestamp of the start of the day wrt. the timezone.
export function toDayTimeMsWrtTimezone(dateISOString: string): number {
  return new Date(`${dateISOString}T${TIMEZONE_OFFSET_STRING}:00Z`).valueOf();
}
