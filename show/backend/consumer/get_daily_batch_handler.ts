import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { BATCH_SIZE_OF_DAILY_PROCESSING_CONSUMERS } from "../../../common/params";
import { Table } from "@google-cloud/bigtable";
import { GetDailyBatchHandlerInterface } from "@phading/product_meter_service_interface/show/backend/consumer/handler";
import {
  GetDailyBatchRequestBody,
  GetDailyBatchResponse,
} from "@phading/product_meter_service_interface/show/backend/consumer/interface";

export class GetDailyBatchHandler extends GetDailyBatchHandlerInterface {
  public static create(): GetDailyBatchHandler {
    return new GetDailyBatchHandler(
      BATCH_SIZE_OF_DAILY_PROCESSING_CONSUMERS,
      BIGTABLE,
      () => new Date(),
    );
  }

  public constructor(
    private batchSize: number,
    private bigtable: Table,
    private getNowDate: () => Date,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: GetDailyBatchRequestBody,
  ): Promise<GetDailyBatchResponse> {
    // Do not process today's data.
    let end = `t1#${toDateISOString(toToday(this.getNowDate()))}`;
    // Add "0" to skip the start cursor.
    let start = body.cursor ? body.cursor + "0" : `t1#`;
    let [rows] = await this.bigtable.getRows({
      start,
      end,
      limit: this.batchSize,
      filter: [
        {
          row: {
            cellLimit: 1,
          },
        },
        {
          value: {
            strip: true,
          },
        },
      ],
    });
    let rowKeys = rows.map((row) => row.id);
    return {
      rowKeys,
      cursor:
        rowKeys.length === this.batchSize
          ? rowKeys[rowKeys.length - 1]
          : undefined,
    };
  }
}
