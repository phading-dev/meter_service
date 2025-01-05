import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import {
  CACHED_SESSION_EXCHANGER,
  CachedSessionExchanger,
} from "./common/cached_session_exchanger";
import { Table } from "@google-cloud/bigtable";
import { RecordWatchTimeHandlerInterface } from "@phading/product_meter_service_interface/show/web/consumer/handler";
import {
  RecordWatchTimeRequestBody,
  RecordWatchTimeResponse,
} from "@phading/product_meter_service_interface/show/web/consumer/interface";
import { newBadRequestError, newNotAcceptableError } from "@selfage/http_error";

export class RecordWatchTimeHandler extends RecordWatchTimeHandlerInterface {
  public static create(): RecordWatchTimeHandler {
    return new RecordWatchTimeHandler(
      BIGTABLE,
      CACHED_SESSION_EXCHANGER,
      () => new Date(),
    );
  }

  private static ONE_MONTH_IN_MS = 30 * 24 * 60 * 60 * 1000;

  public constructor(
    private bigtable: Table,
    private sessionExchanger: CachedSessionExchanger,
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
    let accountId = await this.sessionExchanger.getAccountId(
      sessionStr,
      "record watch time",
    );
    let today = toDateISOString(toToday(this.getNowDate()));
    await this.bigtable.row(`t1#${today}#${accountId}`).save({
      c: {
        p: {
          value: "",
        },
      },
    });
    await this.bigtable
      .row(`d1#${today}#${accountId}`)
      .increment(`w:${body.seasonId}#${body.episodeId}#w`, body.watchTimeMs);
    return {};
  }
}
