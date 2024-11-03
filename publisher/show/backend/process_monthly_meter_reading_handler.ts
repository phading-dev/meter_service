import { BIGTABLE } from "../../../common/bigtable";
import {
  incrementColumn,
  normalizeData,
} from "../../../common/bigtable_data_helper";
import {
  toMonthISOString,
  toMonthTimeMsWrtTimezone,
} from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { generateEarningsStatement } from "@phading/commerce_service_interface/publisher/show/backend/client";
import { ProductType } from "@phading/price";
import { ProcessMonthlyMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/publisher/show/backend/handler";
import {
  ProcessMonthlyMeterReadingRequestBody,
  ProcessMonthlyMeterReadingResponse,
} from "@phading/product_meter_service_interface/publisher/show/backend/interface";
import {
  getStorageMeterReading,
  getUploadMeterReading,
} from "@phading/product_service_interface/publisher/show/backend/client";
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
    private interruptAfterCheckPoint: () => void = () => {},
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
    await this.bigtable.deleteRows(`t5#${month}#${accountId}`);
    await this.writeOutputRowsAndGenerateTransaction(
      month,
      accountId,
      data["t"]["w"].value,
      data["t"]["mb"].value,
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
    let end = `t5#${month}#${accountId}+`;
    let start = `t5#${month}#${accountId}`;
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
    data["c"]["p"].value = "1";
    await this.bigtable.insert([
      {
        key: rowKey,
        data,
      },
    ]);
    this.interruptAfterCheckPoint();
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
        items: [
          {
            productType: ProductType.SHOW,
            quantity: totalWatchTimeSec,
          },
          {
            productType: ProductType.PLATFORM_CUT_SHOW,
            quantity: totalWatchTimeSec * -1,
          },
          {
            productType: ProductType.NETWORK,
            quantity: totalMbs * -1,
          },
          {
            productType: ProductType.STORAGE,
            quantity: storageResponse.mbh * -1,
          },
          {
            productType: ProductType.UPLAOD,
            quantity: uploadResponse.mb * -1,
          },
        ],
      }),
    ]);
  }
}
