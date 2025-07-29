import { BIGTABLE } from "../../../common/bigtable";
import { ENV_VARS } from "../../../env_vars";
import {
  CACHED_SESSION_FETCHER,
  CachedSessionFetcher,
} from "./common/cached_session_fetcher";
import { Table } from "@google-cloud/bigtable";
import { RecordWatchTimeHandlerInterface } from "@phading/meter_service_interface/show/web/consumer/handler";
import {
  RecordWatchTimeRequestBody,
  RecordWatchTimeResponse,
} from "@phading/meter_service_interface/show/web/consumer/interface";
import { newBadRequestError, newNotAcceptableError } from "@selfage/http_error";
import { TzDate } from "@selfage/tz_date";

export class RecordWatchTimeHandler extends RecordWatchTimeHandlerInterface {
  public static create(): RecordWatchTimeHandler {
    return new RecordWatchTimeHandler(
      BIGTABLE,
      CACHED_SESSION_FETCHER,
      () => new Date(),
    );
  }

  private static ONE_MONTH_IN_MS = 30 * 24 * 60 * 60 * 1000;

  public constructor(
    private bigtable: Table,
    private sessionFetcher: CachedSessionFetcher,
    private getNowDate: () => Date,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: RecordWatchTimeRequestBody,
    sessionStr: string,
  ): Promise<RecordWatchTimeResponse> {
    if (!body.seasonId) {
      throw newBadRequestError(`"seasonId" is required.`);
    }
    if (!body.episodeId) {
      throw newBadRequestError(`"episodeId" is required.`);
    }
    if (!body.watchTimeMs) {
      throw newBadRequestError(`"watchTimeMs" is required.`);
    }
    if (body.watchTimeMs > RecordWatchTimeHandler.ONE_MONTH_IN_MS) {
      throw newNotAcceptableError(
        `"watchTimeMs" is unreasonably large, which is ${body.watchTimeMs}. It could be a bad actor.`,
      );
    }
    if (body.watchTimeMs < 0) {
      throw newBadRequestError(`"watchTimeMs" cannot be negative.`);
    }
    let accountId = await this.sessionFetcher.getAccountId(
      sessionStr,
      "record watch time",
    );
    // TODO: Cleanup later when system is stable.
    console.log(`${loggingPrefix} Record watch time ${JSON.stringify(body)} for accountId ${accountId}`);
    let todayString = TzDate.fromNewDate(
      this.getNowDate(),
      ENV_VARS.timezoneNegativeOffset,
    ).toLocalDateISOString();
    await this.bigtable.row(`t1#${todayString}#${accountId}`).save({
      c: {
        p: {
          value: "",
        },
      },
    });
    await this.bigtable
      .row(`d1#${todayString}#${accountId}`)
      .increment(`w:${body.seasonId}#${body.episodeId}#w`, body.watchTimeMs);
    return {};
  }
}
