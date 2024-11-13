import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { BATCH_SIZE_OF_MONTHLY_RPOCESSING_PUBLISHERS } from "../../../common/params";
import { Table } from "@google-cloud/bigtable";
import { GetMonthlyBatchHandlerInterface } from "@phading/product_meter_service_interface/show/backend/publisher/handler";
import {
  GetMonthlyBatchRequestBody,
  GetMonthlyBatchResponse,
} from "@phading/product_meter_service_interface/show/backend/publisher/interface";

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
    let end = `q5#${endMonth}`;
    let start = body.cursor ? body.cursor + "0" : "q5#";
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

  // Either this month or the month of the first unprocessed date from q1# rows or q3# rows.
  private async getEndMonth(): Promise<string> {
    let todayString = toDateISOString(toToday(this.getNowDate()));
    let q1End = `q1#${todayString}`;
    let q1Start = `q1#`;
    let q3End = `q3#${todayString}`;
    let q3Start = `q3#`;
    let [[q1Rows], [q3Rows]] = await Promise.all([
      this.bigtable.getRows({
        start: q1Start,
        end: q1End,
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
        start: q3Start,
        end: q3End,
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
    let q1EndDate =
      q1Rows.length === 0 ? todayString : q1Rows[0].id.split("#")[1];
    let q3EnDate =
      q3Rows.length === 0 ? todayString : q3Rows[0].id.split("#")[1];
    let endDate =
      new Date(q1EndDate).valueOf() < new Date(q3EnDate).valueOf()
        ? q1EndDate
        : q3EnDate;
    let [year, month] = endDate.split("-");
    return `${year}-${month}`;
  }
}
