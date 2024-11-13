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
    let end = `q3#${endDate}`;
    // `!` sign is smaller than `#` sign, so it can mark the start of the range even with #${checkpointId}.
    let start = body.cursor ? body.cursor + "!" : "q3#";
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

  // Either today or the unprocessed date from q1# rows.
  private async getEndDate(): Promise<string> {
    let todayString = toDateISOString(toToday(this.getNowDate()));
    let end = `q1#${todayString}`;
    let start = `q1#`;
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
