import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { MAX_MEDIA_CONTENT_LENGTH } from "@phading/constants/video";
import { RecordStorageStartHandlerInterface } from "@phading/product_meter_service_interface/show/web/publisher/handler";
import {
  RecordStorageStartRequestBody,
  RecordStorageStartResponse,
} from "@phading/product_meter_service_interface/show/web/publisher/interface";
import { exchangeSessionAndCheckCapability } from "@phading/user_session_service_interface/node/client";
import {
  newBadRequestError,
  newNotAcceptableError,
  newUnauthorizedError,
} from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

export class RecordStorageStartHandler extends RecordStorageStartHandlerInterface {
  public static create(): RecordStorageStartHandler {
    return new RecordStorageStartHandler(
      BIGTABLE,
      SERVICE_CLIENT,
      () => new Date(),
    );
  }

  private static DOUBLE_MEDIA_CONTENT_LENGTH = MAX_MEDIA_CONTENT_LENGTH * 2;
  private static ONE_MONTH_IN_MS = 30 * 24 * 60 * 60 * 1000;
  private static ONE_HOUR_IN_MS = 60 * 60 * 1000;

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
    private getNowDate: () => Date,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: RecordStorageStartRequestBody,
    sessionStr: string,
  ): Promise<RecordStorageStartResponse> {
    if (!body.name) {
      throw newBadRequestError(`"name" is required.`);
    }
    if (!body.storageBytes) {
      throw newBadRequestError(`"storageStartBytes" is required.`);
    }
    if (!body.storageStartMs) {
      throw newBadRequestError(`"storageStartMs" is required.`);
    }
    if (
      body.storageBytes > RecordStorageStartHandler.DOUBLE_MEDIA_CONTENT_LENGTH
    ) {
      throw newNotAcceptableError(
        `"storageBytes" is unreasonably large, which is ${body.storageBytes}. It could be a bad actor.`,
      );
    }
    let nowDate = this.getNowDate();
    if (
      body.storageStartMs <
      nowDate.valueOf() - RecordStorageStartHandler.ONE_MONTH_IN_MS
    ) {
      throw newNotAcceptableError(
        `"storageStartMs" is unreasonably small, which is ${body.storageStartMs}. It could be a bad actor.`,
      );
    }
    if (
      body.storageStartMs >
      nowDate.valueOf() + RecordStorageStartHandler.ONE_HOUR_IN_MS
    ) {
      throw newNotAcceptableError(
        `"storageStartMs" is unreasonably large, which is ${body.storageStartMs}. It could be a bad actor.`,
      );
    }
    let { accountId, canPublishShows } =
      await exchangeSessionAndCheckCapability(this.serviceClient, {
        signedSession: sessionStr,
        checkCanPublishShows: true,
      });
    if (!canPublishShows) {
      throw newUnauthorizedError(
        `Account ${accountId} not allowed to record storage start.`,
      );
    }
    let today = toDateISOString(toToday(nowDate));
    await this.bigtable.row(`t6#${today}#${accountId}`).save({
      c: {
        p: {
          value: "",
        },
      },
    });
    await this.bigtable.row(`d6#${today}#${accountId}`).save({
      s: {
        [`${body.name}#b`]: body.storageBytes,
        [`${body.name}#s`]: body.storageStartMs,
      },
    });
    return {};
  }
}
