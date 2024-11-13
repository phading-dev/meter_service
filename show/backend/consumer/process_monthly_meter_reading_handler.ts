import { BIGTABLE } from "../../../common/bigtable";
import { incrementColumn } from "../../../common/bigtable_data_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { generateBillingStatement } from "@phading/commerce_service_interface/backend/consumer/client";
import { MeterType } from "@phading/commerce_service_interface/backend/consumer/interface";
import { ProcessMonthlyMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/show/backend/consumer/handler";
import {
  ProcessMonthlyMeterReadingRequestBody,
  ProcessMonthlyMeterReadingResponse,
} from "@phading/product_meter_service_interface/show/backend/consumer/interface";
import { newBadRequestError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

export class ProcessMonthlyMeterReadingHandler extends ProcessMonthlyMeterReadingHandlerInterface {
  public static create(): ProcessMonthlyMeterReadingHandler {
    return new ProcessMonthlyMeterReadingHandler(BIGTABLE, SERVICE_CLIENT);
  }

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
    // rowKey should be q2#${date}#${consumerId}
    let queueExists = (await this.bigtable.row(body.rowKey).exists())[0];
    if (!queueExists) {
      console.log(
        `${loggingPrefix} row ${body.rowKey} is not found because it has been processed.`,
      );
      return {};
    }

    let [_, month, accountId] = body.rowKey.split("#");
    let data = await this.aggregate(month, accountId);
    await this.writeOutputRowsAndGenerateTransaction(
      month,
      accountId,
      data["t"]["w"].value,
    );
    // Queue is completed.
    await this.bigtable.row(body.rowKey).delete();
    return {};
  }

  private async aggregate(month: string, accountId: string): Promise<any> {
    let data: any = {};
    // `+` sign is larger than `#` sign, so it can mark the end of the range.
    let end = `d2#${month}#${accountId}+`;
    let start = `d2#${month}#${accountId}`;
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
    return data;
  }

  private async writeOutputRowsAndGenerateTransaction(
    month: string,
    accountId: string,
    totalWatchTimeSec: number,
  ): Promise<void> {
    await Promise.all([
      this.bigtable.insert([
        {
          key: `f2#${accountId}#${month}`,
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
