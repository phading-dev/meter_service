import { BIGTABLE } from "../../../common/bigtable";
import { ENV_VARS } from "../../../env_vars";
import { Table } from "@google-cloud/bigtable";
import { RecordUploadedHandlerInterface } from "@phading/meter_service_interface/show/node/publisher/handler";
import {
  RecordUploadedRequestBody,
  RecordUploadedResponse,
} from "@phading/meter_service_interface/show/node/publisher/interface";
import { newBadRequestError } from "@selfage/http_error";
import { TzDate } from "@selfage/tz_date";

export class RecordUploadedHandler extends RecordUploadedHandlerInterface {
  public static create(): RecordUploadedHandler {
    return new RecordUploadedHandler(BIGTABLE, () => new Date());
  }

  public constructor(
    private bigtable: Table,
    private getNowDate: () => Date,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: RecordUploadedRequestBody,
  ): Promise<RecordUploadedResponse> {
    if (!body.name) {
      throw newBadRequestError(`"name" is required.`);
    }
    if (!body.uploadedBytes) {
      throw newBadRequestError(`"uploadedBytes" is required.`);
    }
    let todayString = TzDate.fromDate(
      this.getNowDate(),
      ENV_VARS.timezoneNegativeOffset,
    ).toLocalDateISOString();
    await this.bigtable.insert([
      {
        key: `d6#${todayString}#${body.accountId}`,
        data: {
          u: {
            [body.name]: {
              value: body.uploadedBytes,
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
