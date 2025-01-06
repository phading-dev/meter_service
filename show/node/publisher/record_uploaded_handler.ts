import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { Table } from "@google-cloud/bigtable";
import { RecordUploadedHandlerInterface } from "@phading/product_meter_service_interface/show/node/publisher/handler";
import {
  RecordUploadedRequestBody,
  RecordUploadedResponse,
} from "@phading/product_meter_service_interface/show/node/publisher/interface";
import { newBadRequestError } from "@selfage/http_error";

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
    let today = toDateISOString(toToday(this.getNowDate()));
    await this.bigtable.insert([
      {
        key: `d6#${today}#${body.accountId}`,
        data: {
          u: {
            [body.name]: {
              value: body.uploadedBytes,
            },
          },
        },
      },
      {
        key: `t6#${today}#${body.accountId}`,
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
