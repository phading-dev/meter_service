import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { BATCH_SIZE_OF_DAILY_PROCESSING_PUBLISHERS } from "../../../common/params";
import { Table } from "@google-cloud/bigtable";
import { GetDailyBatchHandlerInterface } from "@phading/product_meter_service_interface/show/backend/publisher/handler";
import {
  GetDailyBatchRequestBody,
  GetDailyBatchResponse,
} from "@phading/product_meter_service_interface/show/backend/publisher/interface";

export class GetDailyBatchHandler extends GetDailyBatchHandlerInterface {
  public static create(): GetDailyBatchHandler {
    return new GetDailyBatchHandler(
      BATCH_SIZE_OF_DAILY_PROCESSING_PUBLISHERS,
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
    let endDate = await this.getEndDate();
    let end = `t4#${endDate}`;
    let start = body.cursor ? body.cursor + "0" : "t4#";
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

  // Either today or the unprocessed date from t1# rows.
  private async getEndDate(): Promise<string> {
    let todayString = toDateISOString(toToday(this.getNowDate()));
    let end = `t1#${todayString}`;
    let start = `t1#`;
    let [rows] = await this.bigtable.getRows({
      start,
      end,
      limit: 1,
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
    return rows.length === 0 ? todayString : rows[0].id.split("#")[1];
  }
}
