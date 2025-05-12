import { BIGTABLE } from "../../../common/bigtable";
import { ENV_VARS } from "../../../env_vars";
import { Table } from "@google-cloud/bigtable";
import { RecordStorageEndHandlerInterface } from "@phading/meter_service_interface/show/node/publisher/handler";
import {
  RecordStorageEndRequestBody,
  RecordStorageEndResponse,
} from "@phading/meter_service_interface/show/node/publisher/interface";
import { newBadRequestError, newNotAcceptableError } from "@selfage/http_error";
import { TzDate } from "@selfage/tz_date";

export class RecordStorageEndHandler extends RecordStorageEndHandlerInterface {
  public static create(): RecordStorageEndHandler {
    return new RecordStorageEndHandler(BIGTABLE, () => new Date());
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
    body: RecordStorageEndRequestBody,
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
    let todayString = TzDate.fromNewDate(
      this.getNowDate(),
      ENV_VARS.timezoneNegativeOffset,
    ).toLocalDateISOString();
    await this.bigtable.insert([
      {
        key: `d6#${todayString}#${body.accountId}`,
        data: {
          s: {
            [`${body.name}#e`]: {
              value: body.storageEndMs,
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
