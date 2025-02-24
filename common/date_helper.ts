import { ENV_VARS } from "../env";

export let TIMEZONE_OFFSET_STRING = ENV_VARS.timezoneNegativeOffset
  .toString()
  .padStart(2, "0");

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
  if (date.getUTCHours() < ENV_VARS.timezoneNegativeOffset) {
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

// Inclusive.
export function getDayDifference(startDate: Date, endDate: Date): number {
  return Math.floor(
    (endDate.valueOf() - startDate.valueOf()) / (24 * 60 * 60 * 1000) + 1,
  );
}

// Inclusive.
export function getMonthDifference(startMonth: Date, endMonth: Date): number {
  return (
    (endMonth.getUTCFullYear() - startMonth.getUTCFullYear()) * 12 +
    endMonth.getUTCMonth() -
    startMonth.getUTCMonth() +
    1
  );
}
