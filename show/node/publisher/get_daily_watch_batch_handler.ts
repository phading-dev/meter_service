import { BIGTABLE } from "../../../common/bigtable";
import { BATCH_SIZE_OF_DAILY_WATCH_PROCESSING_PUBLISHERS } from "../../../common/constants";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { Table } from "@google-cloud/bigtable";
import { GetDailyWatchBatchHandlerInterface } from "@phading/meter_service_interface/show/node/publisher/handler";
import {
  GetDailyWatchBatchRequestBody,
  GetDailyWatchBatchResponse,
} from "@phading/meter_service_interface/show/node/publisher/interface";

export class GetDailyWatchBatchHandler extends GetDailyWatchBatchHandlerInterface {
  public static create(): GetDailyWatchBatchHandler {
    return new GetDailyWatchBatchHandler(
      BATCH_SIZE_OF_DAILY_WATCH_PROCESSING_PUBLISHERS,
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
    body: GetDailyWatchBatchRequestBody,
  ): Promise<GetDailyWatchBatchResponse> {
    let endDate = await this.getEndDate();
    let end = `t3#${endDate}`;
    // `!` sign is smaller than `#` sign, so it can mark the start of the range even with #${checkpointId}.
    let start = body.cursor ? body.cursor + "!" : "t3#";
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
