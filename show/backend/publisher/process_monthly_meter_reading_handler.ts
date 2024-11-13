import { BIGTABLE } from "../../../common/bigtable";
import { incrementColumn } from "../../../common/bigtable_data_helper";
import {
  toMonthISOString,
  toMonthTimeMsWrtTimezone,
} from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { generateEarningsStatement } from "@phading/commerce_service_interface/backend/publisher/client";
import { MeterType } from "@phading/commerce_service_interface/backend/publisher/interface";
import { ProcessMonthlyMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/show/backend/publisher/handler";
import {
  ProcessMonthlyMeterReadingRequestBody,
  ProcessMonthlyMeterReadingResponse,
} from "@phading/product_meter_service_interface/show/backend/publisher/interface";
import {
  getStorageMeterReading,
  getUploadMeterReading,
} from "@phading/product_service_interface/show/backend/client";
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
    // rowKey should be q5#${date}#${consumerId}
    let queueExists = (await this.bigtable.row(body.rowKey).exists())[0];
    if (!queueExists) {
      console.log(
        `${loggingPrefix} row ${body.rowKey} is not found maybe because it has been processed.`,
      );
      return {};
    }

    let [_, month, accountId] = body.rowKey.split("#");
    let data = await this.aggregate(month, accountId);
    await this.writeOutputRowsAndGenerateTransaction(
      month,
      accountId,
      data["t"]["w"].value,
      data["t"]["mb"].value,
    );
    // Queue is completed.
    await this.bigtable.row(body.rowKey).delete();
    return {};
  }

  private async aggregate(month: string, accountId: string): Promise<any> {
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
      incrementColumn(data, "t", "w", row.data["t"]["w"][0].value);
      let mbs = Math.ceil(
        row.data["t"]["kb"][0].value /
          ProcessMonthlyMeterReadingHandler.ONE_MB_IN_KB,
      );
      incrementColumn(data, "t", "mb", mbs);
    }
    return data;
  }

  private async writeOutputRowsAndGenerateTransaction(
    month: string,
    accountId: string,
    totalWatchTimeSec: number,
    totalMbs: number,
  ): Promise<void> {
    let startTimeMs = toMonthTimeMsWrtTimezone(month);
    let date = new Date(month);
    date.setUTCMonth(date.getUTCMonth() + 1);
    let endTimeMs = toMonthTimeMsWrtTimezone(toMonthISOString(date));
    let [storageResponse, uploadResponse] = await Promise.all([
      getStorageMeterReading(this.serviceClient, {
        accountId,
        startTimeMs,
        endTimeMs,
      }),
      getUploadMeterReading(this.serviceClient, {
        accountId,
        startTimeMs,
        endTimeMs,
      }),
    ]);

    await Promise.all([
      this.bigtable.insert([
        {
          key: `f4#${accountId}#${month}`,
          data: {
            t: {
              w: {
                value: totalWatchTimeSec,
              },
              mb: {
                value: totalMbs,
              },
              smbh: {
                value: storageResponse.mbh,
              },
              umb: {
                value: uploadResponse.mb,
              },
            },
          },
        },
      ]),
      generateEarningsStatement(this.serviceClient, {
        accountId,
        month,
        readings: [
          {
            meterType: MeterType.SHOW_WATCH_TIME_SEC,
            reading: totalWatchTimeSec,
          },
          {
            meterType: MeterType.NETWORK_TRANSMITTED_MB,
            reading: totalMbs,
          },
          {
            meterType: MeterType.STORAGE_MB_HOUR,
            reading: storageResponse.mbh,
          },
          {
            meterType: MeterType.UPLOAD_MB,
            reading: uploadResponse.mb,
          },
        ],
      }),
    ]);
  }
}
