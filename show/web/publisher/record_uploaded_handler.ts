import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { MAX_MEDIA_CONTENT_LENGTH } from "@phading/constants/video";
import { RecordUploadedHandlerInterface } from "@phading/product_meter_service_interface/show/web/publisher/handler";
import {
  RecordUploadedRequestBody,
  RecordUploadedResponse,
} from "@phading/product_meter_service_interface/show/web/publisher/interface";
import { exchangeSessionAndCheckCapability } from "@phading/user_session_service_interface/node/client";
import {
  newBadRequestError,
  newNotAcceptableError,
  newUnauthorizedError,
} from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

export class RecordUploadedHandler extends RecordUploadedHandlerInterface {
  public static create(): RecordUploadedHandler {
    return new RecordUploadedHandler(
      BIGTABLE,
      SERVICE_CLIENT,
      () => new Date(),
    );
  }

  private static DOUBLE_MEDIA_CONTENT_LENGTH = MAX_MEDIA_CONTENT_LENGTH * 2;

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
    private getNowDate: () => Date,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: RecordUploadedRequestBody,
    sessionStr: string,
  ): Promise<RecordUploadedResponse> {
    if (!body.name) {
      throw newBadRequestError(`"name" is required.`);
    }
    if (!body.uploadedBytes) {
      throw newBadRequestError(`"uploadedBytes" is required.`);
    }
    if (
      body.uploadedBytes >
      2 * RecordUploadedHandler.DOUBLE_MEDIA_CONTENT_LENGTH
    ) {
      throw newNotAcceptableError(
        `"uploadedBytes" is unreasonably large, which is ${body.uploadedBytes}. It could be a bad actor.`,
      );
    }
    let { accountId, canPublishShows } =
      await exchangeSessionAndCheckCapability(this.serviceClient, {
        signedSession: sessionStr,
        checkCanPublishShows: true,
      });
    if (!canPublishShows) {
      throw newUnauthorizedError(
        `Account ${accountId} not allowed to record uploaded.`,
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
      u: {
        [body.name]: body.uploadedBytes,
      },
    });
    return {};
  }
}
