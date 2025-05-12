import { BIGTABLE } from "../../../common/bigtable";
import { ENV_VARS } from "../../../env_vars";
import { Table } from "@google-cloud/bigtable";
import { RecordStorageStartHandlerInterface } from "@phading/meter_service_interface/show/node/publisher/handler";
import {
  RecordStorageStartRequestBody,
  RecordStorageStartResponse,
} from "@phading/meter_service_interface/show/node/publisher/interface";
import { newBadRequestError, newNotAcceptableError } from "@selfage/http_error";
import { TzDate } from "@selfage/tz_date";

export class RecordStorageStartHandler extends RecordStorageStartHandlerInterface {
  public static create(): RecordStorageStartHandler {
    return new RecordStorageStartHandler(BIGTABLE, () => new Date());
  }

  private static ONE_MONTH_IN_MS = 30 * 24 * 60 * 60 * 1000;
  private static ONE_HOUR_IN_MS = 60 * 60 * 1000;

  public constructor(
    private bigtable: Table,
    private getNowDate: () => Date,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: RecordStorageStartRequestBody,
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
    let nowDate = this.getNowDate();
    if (
      body.storageStartMs <
      nowDate.valueOf() - RecordStorageStartHandler.ONE_MONTH_IN_MS
    ) {
      throw newNotAcceptableError(
        `"storageStartMs" is unreasonably small, which is ${body.storageStartMs}.`,
      );
    }
    if (
      body.storageStartMs >
      nowDate.valueOf() + RecordStorageStartHandler.ONE_HOUR_IN_MS
    ) {
      throw newNotAcceptableError(
        `"storageStartMs" is unreasonably large, which is ${body.storageStartMs}.`,
      );
    }
    let todayString = TzDate.fromNewDate(
      this.getNowDate(),
      ENV_VARS.timezoneNegativeOffset,
    ).toLocalDateISOString();
    await this.bigtable.insert([
      {
        key: `d6#${todayString}#${body.accountId}`,
        data: {
          s: {
            [`${body.name}#b`]: {
              value: body.storageBytes,
            },
            [`${body.name}#s`]: {
              value: body.storageStartMs,
            },
          },
        },
      },
      {
        key: `t6#${todayString}#${body.accountId}`,
        data: {
          c: {
            p: {
              value: "",
            },
          },
        },
      },
    ]);
    return {};
  }
}
