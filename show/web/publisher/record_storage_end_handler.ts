import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { RecordStorageEndHandlerInterface } from "@phading/product_meter_service_interface/show/web/publisher/handler";
import {
  RecordStorageEndRequestBody,
  RecordStorageEndResponse,
} from "@phading/product_meter_service_interface/show/web/publisher/interface";
import { exchangeSessionAndCheckCapability } from "@phading/user_session_service_interface/node/client";
import {
  newBadRequestError,
  newNotAcceptableError,
  newUnauthorizedError,
} from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

export class RecordStorageEndHandler extends RecordStorageEndHandlerInterface {
  public static create(): RecordStorageEndHandler {
    return new RecordStorageEndHandler(
      BIGTABLE,
      SERVICE_CLIENT,
      () => new Date(),
    );
  }

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
    body: RecordStorageEndRequestBody,
    sessionStr: string,
  ): Promise<RecordStorageEndResponse> {
    if (!body.name) {
      throw newBadRequestError(`"name" is required.`);
    }
    if (!body.storageEndMs) {
      throw newBadRequestError(`"storageEndMs" is required.`);
    }
    let nowDate = this.getNowDate();
    if (
      body.storageEndMs <
      nowDate.valueOf() - RecordStorageEndHandler.ONE_MONTH_IN_MS
    ) {
      throw newNotAcceptableError(
        `"storageEndMs" is unreasonably small, which is ${body.storageEndMs}. It could be a bad actor.`,
      );
    }
    if (
      body.storageEndMs >
      nowDate.valueOf() + RecordStorageEndHandler.ONE_HOUR_IN_MS
    ) {
      throw newNotAcceptableError(
        `"storageEndMs" is unreasonably large, which is ${body.storageEndMs}. It could be a bad actor.`,
      );
    }
    let { accountId, canPublishShows } =
      await exchangeSessionAndCheckCapability(this.serviceClient, {
        signedSession: sessionStr,
        checkCanPublishShows: true,
      });
    if (!canPublishShows) {
      throw newUnauthorizedError(
        `Account ${accountId} not allowed to record storage end.`,
      );
    }
    let today = toDateISOString(toToday(this.getNowDate()));
    await this.bigtable.row(`t6#${today}#${accountId}`).save({
      c: {
        p: {
          value: "",
        },
      },
    });
    await this.bigtable.row(`d6#${today}#${accountId}`).save({
      s: {
        [`${body.name}#e`]: body.storageEndMs,
      },
    });
    return {};
  }
}
