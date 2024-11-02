import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { SyncMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/consumer/show/frontend/handler";
import {
  SyncMeterReadingRequestBody,
  SyncMeterReadingResponse,
} from "@phading/product_meter_service_interface/consumer/show/frontend/interface";
import { exchangeSessionAndCheckCapability } from "@phading/user_session_service_interface/backend/client";
import { newBadRequestError, newUnauthorizedError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { LRUCache } from "lru-cache";

export class SyncMeterReadingHandler extends SyncMeterReadingHandlerInterface {
  public static create(): SyncMeterReadingHandler {
    return new SyncMeterReadingHandler(
      BIGTABLE,
      SERVICE_CLIENT,
      () => new Date(),
    );
  }

  private lruCache: LRUCache<string, string>;

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
    private getNowDate: () => Date,
  ) {
    super();
    this.lruCache = new LRUCache({
      max: 10000,
      ttl: 60000, // ms
    });
  }

  public async handle(
    loggingPrefix: string,
    body: SyncMeterReadingRequestBody,
    sessionStr: string,
  ): Promise<SyncMeterReadingResponse> {
    if (!body.seasonId) {
      throw newBadRequestError(`"seasonId" is required.`);
    }
    if (!body.episodeId) {
      throw newBadRequestError(`"episodeId" is required.`);
    }
    if (!body.watchTimeMs) {
      throw newBadRequestError(`"watchTimeMs" is required.`);
    }
    let accountId = this.lruCache.get(sessionStr);
    if (!accountId) {
      let { userSession, canConsumeShows } =
        await exchangeSessionAndCheckCapability(this.serviceClient, {
          signedSession: sessionStr,
          checkCanConsumeShows: true,
        });
      if (!canConsumeShows) {
        throw newUnauthorizedError(
          `Account ${userSession.accountId} not allowed to sync meters of watch time.`,
        );
      }
      this.lruCache.set(sessionStr, userSession.accountId);
      accountId = userSession.accountId;
    }
    let today = toDateISOString(toToday(this.getNowDate()));
    await this.bigtable
      .row(`t1#${today}#${accountId}`)
      .increment(`w:${body.seasonId}#${body.episodeId}`, body.watchTimeMs);
    return {};
  }
}
