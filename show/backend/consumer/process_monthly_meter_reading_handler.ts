import { BIGTABLE } from "../../../common/bigtable";
import {
  incrementColumn,
  normalizeData,
} from "../../../common/bigtable_data_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { generateBillingStatement } from "@phading/commerce_service_interface/backend/consumer/client";
import { MeterType } from "@phading/commerce_service_interface/backend/consumer/interface";
import { ProcessMonthlyMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/show/backend/consumer/handler";
import {
  ProcessMonthlyMeterReadingRequestBody,
  ProcessMonthlyMeterReadingResponse,
} from "@phading/product_meter_service_interface/show/backend/consumer/interface";
import {
  newBadRequestError,
  newInternalServerErrorError,
} from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

export class ProcessMonthlyMeterReadingHandler extends ProcessMonthlyMeterReadingHandlerInterface {
  public static create(): ProcessMonthlyMeterReadingHandler {
    return new ProcessMonthlyMeterReadingHandler(BIGTABLE, SERVICE_CLIENT);
  }

  public interfereBeforeCheckPoint: () => Promise<void> = () =>
    Promise.resolve();
  public interruptAfterCheckPoint: () => void = () => {};

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessMonthlyMeterReadingRequestBody,
  ): Promise<ProcessMonthlyMeterReadingResponse> {
    if (!body.rowKey) {
      throw newBadRequestError(`"rowKey" is required.`);
    }
    let [rows] = await this.bigtable.getRows({
      keys: [body.rowKey],
      filter: {
        column: {
          cellLimit: 1,
        },
      },
    });
    if (rows.length === 0) {
      console.log(
        `${loggingPrefix} row ${body.rowKey} is not found maybe because it has been processed.`,
      );
      return {};
    }
    let [_, month, accountId] = body.rowKey.split("#");
    let data = normalizeData(rows[0].data);
    if (!data["c"]["p"].value) {
      await this.aggregateAndCheckPoint(body.rowKey, month, accountId, data);
    }
    // Cleans up data rows.
    await this.bigtable.deleteRows(`t2#${month}#${accountId}`);
    await this.writeOutputRowsAndGenerateTransaction(
      month,
      accountId,
      data["t"]["w"].value,
    );
    // Marks the completion.
    await this.bigtable.row(body.rowKey).delete();
    return {};
  }

  private async aggregateAndCheckPoint(
    rowKey: string,
    month: string,
    accountId: string,
    data: any,
  ): Promise<void> {
    // `+` sign is larger than `#` sign, so it can mark the end of the range.
    let end = `t2#${month}#${accountId}+`;
    let start = `t2#${month}#${accountId}`;
    let [rows] = await this.bigtable.getRows({
      start,
      end,
      filter: {
        column: {
          cellLimit: 1,
        },
      },
    });
    for (let row of rows) {
      incrementColumn(data, "t", "w", row.data["t"]["w"][0].value);
    }
    data["c"]["p"].value = "1";
    let row = this.bigtable.row(rowKey);
    await this.interfereBeforeCheckPoint();
    // Conditionally write the data only if c:p is still empty.
    let [matched] = await row.filter(
      [
        {
          family: /^c$/,
        },
        {
          column: /^p$/,
        },
        {
          column: {
            cellLimit: 1,
          },
        },
        {
          value: /^$/,
        },
      ],
      {
        onMatch: [
          {
            method: "insert",
            data,
          },
        ],
      },
    );
    if (!matched) {
      throw newInternalServerErrorError(`Row ${rowKey} is already completed.`);
    }
    this.interruptAfterCheckPoint();
  }

  private async writeOutputRowsAndGenerateTransaction(
    month: string,
    accountId: string,
    totalWatchTimeSec: number,
  ): Promise<void> {
    await Promise.all([
      this.bigtable.insert([
        {
          key: `f3#${accountId}#${month}`,
          data: {
            t: {
              w: {
                value: totalWatchTimeSec,
              },
            },
          },
        },
      ]),
      generateBillingStatement(this.serviceClient, {
        accountId,
        month,
        readings: [
          {
            meterType: MeterType.SHOW_WATCH_TIME_SEC,
            reading: totalWatchTimeSec,
          },
        ],
      }),
    ]);
  }
}
