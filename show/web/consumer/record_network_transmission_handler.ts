import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import {
  CACHED_SESSION_FETCHER,
  CachedSessionFetcher,
} from "./common/cached_session_fetcher";
import { Table } from "@google-cloud/bigtable";
import { RecordNetworkTransmissionHandlerInterface } from "@phading/meter_service_interface/show/web/consumer/handler";
import {
  RecordNetworkTransmissionRequestBody,
  RecordNetworkTransmissionResponse,
} from "@phading/meter_service_interface/show/web/consumer/interface";
import { newBadRequestError, newNotAcceptableError } from "@selfage/http_error";

export class RecordNetworkTransmissionHandler extends RecordNetworkTransmissionHandlerInterface {
  public static create(): RecordNetworkTransmissionHandler {
    return new RecordNetworkTransmissionHandler(
      BIGTABLE,
      CACHED_SESSION_FETCHER,
      () => new Date(),
    );
  }

  private static TEN_TB = 10 * 1024 * 1024 * 1024 * 1024;

  public constructor(
    private bigtable: Table,
    private sessionFetcher: CachedSessionFetcher,
    private getNowDate: () => Date,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: RecordNetworkTransmissionRequestBody,
    sessionStr: string,
  ): Promise<RecordNetworkTransmissionResponse> {
    if (!body.seasonId) {
      throw newBadRequestError(`"seasonId" is required.`);
    }
    if (!body.episodeId) {
      throw newBadRequestError(`"episodeId" is required.`);
    }
    if (!body.transmittedBytes) {
      throw newBadRequestError(`"transmittedBytes" is required.`);
    }
    if (body.transmittedBytes > RecordNetworkTransmissionHandler.TEN_TB) {
      throw newNotAcceptableError(
        `"transmittedBytes" is unreasonably large, which is ${body.transmittedBytes}. It could be a bad actor.`,
      );
    }
    let accountId = await this.sessionFetcher.getAccountId(
      sessionStr,
      "record network transmission",
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
      .increment(
        `w:${body.seasonId}#${body.episodeId}#n`,
        body.transmittedBytes,
      );
    return {};
  }
}
