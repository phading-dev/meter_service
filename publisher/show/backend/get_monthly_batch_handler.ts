import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { BATCH_SIZE_OF_MONTHLY_RPOCESSING_PUBLISHERS } from "../../../common/params";
import { Table } from "@google-cloud/bigtable";
import { GetMonthlyBatchHandlerInterface } from "@phading/product_meter_service_interface/publisher/show/backend/handler";
import {
  GetMonthlyBatchRequestBody,
  GetMonthlyBatchResponse,
} from "@phading/product_meter_service_interface/publisher/show/backend/interface";

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
    let end = `t7#${endMonth}`;
    let start = body.cursor ? body.cursor + "0" : "t7#";
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

  // Either this month or the month of the first unprocessed date from t1# rows or t4# rows.
  private async getEndMonth(): Promise<string> {
    let todayString = toDateISOString(toToday(this.getNowDate()));
    let t1End = `t1#${todayString}`;
    let t1Start = `t1#`;
    let t4End = `t4#${todayString}`;
    let t4Start = `t4#`;
    let [[t1Rows], [t4Rows]] = await Promise.all([
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
        start: t4Start,
        end: t4End,
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
    let t4EnDate =
      t4Rows.length === 0 ? todayString : t4Rows[0].id.split("#")[1];
    let endDate =
      new Date(t1EndDate).valueOf() < new Date(t4EnDate).valueOf()
        ? t1EndDate
        : t4EnDate;
    let [year, month] = endDate.split("-");
    return `${year}-${month}`;
  }
}
