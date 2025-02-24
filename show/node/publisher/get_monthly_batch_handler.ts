import { BIGTABLE } from "../../../common/bigtable";
import { BATCH_SIZE_OF_MONTHLY_RPOCESSING_PUBLISHERS } from "../../../common/constants";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { Table } from "@google-cloud/bigtable";
import { GetMonthlyBatchHandlerInterface } from "@phading/product_meter_service_interface/show/node/publisher/handler";
import {
  GetMonthlyBatchRequestBody,
  GetMonthlyBatchResponse,
} from "@phading/product_meter_service_interface/show/node/publisher/interface";

export class GetMonthlyBatchHandler extends GetMonthlyBatchHandlerInterface {
  public static create(): GetMonthlyBatchHandler {
    return new GetMonthlyBatchHandler(
      BATCH_SIZE_OF_MONTHLY_RPOCESSING_PUBLISHERS,
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
    body: GetMonthlyBatchRequestBody,
  ): Promise<GetMonthlyBatchResponse> {
    let endMonth = await this.getEndMonth();
    let end = `t5#${endMonth}`;
    let start = body.cursor ? body.cursor + "0" : "t5#";
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

  // Either this month or the month of the first unprocessed date from t1# rows or t3# rows or t6# rows.
  private async getEndMonth(): Promise<string> {
    let todayString = toDateISOString(toToday(this.getNowDate()));
    let t1End = `t1#${todayString}`;
    let t1Start = `t1#`;
    let t3End = `t3#${todayString}`;
    let t3Start = `t3#`;
    let t6End = `t6#${todayString}`;
    let t6Start = `t6#`;
    let [[t1Rows], [t3Rows], [t6Rows]] = await Promise.all([
      this.bigtable.getRows({
        start: t1Start,
        end: t1End,
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
      }),
      this.bigtable.getRows({
        start: t3Start,
        end: t3End,
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
      }),
      this.bigtable.getRows({
        start: t6Start,
        end: t6End,
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
      }),
    ]);
    let t1EndDate =
      t1Rows.length === 0 ? todayString : t1Rows[0].id.split("#")[1];
    let t3EnDate =
      t3Rows.length === 0 ? todayString : t3Rows[0].id.split("#")[1];
    let t6EndDate =
      t6Rows.length === 0 ? todayString : t6Rows[0].id.split("#")[1];
    let endDate = [t1EndDate, t3EnDate, t6EndDate].sort()[0];
    let [year, month] = endDate.split("-");
    return `${year}-${month}`;
  }
}
