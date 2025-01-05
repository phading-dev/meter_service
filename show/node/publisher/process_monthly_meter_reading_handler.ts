import { BIGTABLE } from "../../../common/bigtable";
import { incrementColumn } from "../../../common/bigtable_data_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { generateEarningsStatement } from "@phading/commerce_service_interface/node/publisher/client";
import { MeterType } from "@phading/commerce_service_interface/node/publisher/interface";
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
      if (row.data["t"]["ws"]) {
        incrementColumn(data, "t", "ws", row.data["t"]["ws"][0].value);
      }
      if (row.data["t"]["nk"]) {
        let mb = Math.ceil(
          row.data["t"]["nk"][0].value /
            ProcessMonthlyMeterReadingHandler.ONE_MB_IN_KB,
        );
        incrementColumn(data, "t", "nm", mb);
      }
      if (row.data["t"]["uk"]) {
        let mb = Math.ceil(
          row.data["t"]["uk"][0].value /
            ProcessMonthlyMeterReadingHandler.ONE_MB_IN_KB,
        );
        incrementColumn(data, "t", "um", mb);
      }
      if (row.data["t"]["smm"]) {
        incrementColumn(
          data,
          "t",
          "smh",
          Math.ceil(row.data["t"]["smm"][0].value / 60),
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
            reading: data["t"]["ws"] ? data["t"]["ws"].value : 0,
          },
          {
            meterType: MeterType.NETWORK_TRANSMITTED_MB,
            reading: data["t"]["nm"] ? data["t"]["nm"].value : 0,
          },
          {
            meterType: MeterType.UPLOADED_MB,
            reading: data["t"]["um"] ? data["t"]["um"].value : 0,
          },
          {
            meterType: MeterType.STORAGE_MB_HOUR,
            reading: data["t"]["smh"] ? data["t"]["smh"].value : 0,
          },
        ],
      }),
    ]);
  }
}
