import { BIGTABLE } from "../../../common/bigtable";
import { incrementColumn } from "../../../common/bigtable_data_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { generateEarningsStatement } from "@phading/commerce_service_interface/backend/publisher/client";
import { MeterType } from "@phading/commerce_service_interface/backend/publisher/interface";
import { ProcessMonthlyMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/show/node/publisher/handler";
import {
  ProcessMonthlyMeterReadingRequestBody,
  ProcessMonthlyMeterReadingResponse,
} from "@phading/product_meter_service_interface/show/node/publisher/interface";
import { newBadRequestError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

export class ProcessMonthlyMeterReadingHandler extends ProcessMonthlyMeterReadingHandlerInterface {
  public static create(): ProcessMonthlyMeterReadingHandler {
    return new ProcessMonthlyMeterReadingHandler(BIGTABLE, SERVICE_CLIENT);
  }

  private static ONE_MB_IN_KB = 1024;

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
    // rowKey should be t5#${date}#${consumerId}
    let queueExists = (await this.bigtable.row(body.rowKey).exists())[0];
    if (!queueExists) {
      console.log(
        `${loggingPrefix} row ${body.rowKey} is not found maybe because it has been processed.`,
      );
      return {};
    }

    let [_, month, accountId] = body.rowKey.split("#");
    await this.aggregateAndWriteOutputRowsAndGenerateTransaction(
      month,
      accountId,
    );
    // Task is completed.
    await this.bigtable.row(body.rowKey).delete();
    return {};
  }

  private async aggregateAndWriteOutputRowsAndGenerateTransaction(
    month: string,
    accountId: string,
  ): Promise<void> {
    let data: any = {};
    // `+` sign is larger than `#` sign, so it can mark the end of the range.
    let end = `d5#${month}#${accountId}+`;
    let start = `d5#${month}#${accountId}`;
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
      if (row.data["t"]["w"]) {
        incrementColumn(data, "t", "w", row.data["t"]["w"][0].value);
      }
      if (row.data["t"]["n"]) {
        let mbs = Math.ceil(
          row.data["t"]["n"][0].value /
            ProcessMonthlyMeterReadingHandler.ONE_MB_IN_KB,
        );
        incrementColumn(data, "t", "n", mbs);
      }
      if (row.data["t"]["u"]) {
        let mbs = Math.ceil(
          row.data["t"]["u"][0].value /
            ProcessMonthlyMeterReadingHandler.ONE_MB_IN_KB,
        );
        incrementColumn(data, "t", "u", mbs);
      }
      if (row.data["t"]["s"]) {
        incrementColumn(
          data,
          "t",
          "s",
          Math.ceil(row.data["t"]["s"][0].value / 60),
        );
      }
    }

    await Promise.all([
      this.bigtable.insert([
        {
          key: `f4#${accountId}#${month}`,
          data: data,
        },
      ]),
      generateEarningsStatement(this.serviceClient, {
        accountId,
        month,
        readings: [
          {
            meterType: MeterType.SHOW_WATCH_TIME_SEC,
            reading: data["t"]["w"] ? data["t"]["w"].value : 0,
          },
          {
            meterType: MeterType.NETWORK_TRANSMITTED_MB,
            reading: data["t"]["n"] ? data["t"]["n"].value : 0,
          },
          {
            meterType: MeterType.UPLOAD_MB,
            reading: data["t"]["u"] ? data["t"]["u"].value : 0,
          },
          {
            meterType: MeterType.STORAGE_MB_HOUR,
            reading: data["t"]["s"] ? data["t"]["s"].value : 0,
          },
        ],
      }),
    ]);
  }
}
