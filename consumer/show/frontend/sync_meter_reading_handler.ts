import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import {
  CACHE_SIZE_OF_SESSION,
  CACHE_TTL_MS_OF_SESSION,
} from "../../../common/params";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { SyncMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/consumer/show/frontend/handler";
import {
  SyncMeterReadingRequestBody,
  SyncMeterReadingResponse,
} from "@phading/product_meter_service_interface/consumer/show/frontend/interface";
import { exchangeSessionAndCheckCapability } from "@phading/user_session_service_interface/backend/client";
import {
  newBadRequestError,
  newNotAcceptableError,
  newUnauthorizedError,
} from "@selfage/http_error";
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

  private static ONE_MONTH_IN_MS = 30 * 24 * 60 * 60 * 1000;
  private lruCache: LRUCache<string, string>;

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
    private getNowDate: () => Date,
  ) {
    super();
    this.lruCache = new LRUCache({
      max: CACHE_SIZE_OF_SESSION,
      ttl: CACHE_TTL_MS_OF_SESSION,
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
    if (body.watchTimeMs > SyncMeterReadingHandler.ONE_MONTH_IN_MS) {
      throw newNotAcceptableError(
        `"watchTimeMs" is unreasonably large, which is ${body.watchTimeMs}. It could be a bad actor.`,
      );
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
