import { BIGTABLE } from "../../../common/bigtable";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { generateBillingStatement } from "@phading/commerce_service_interface/consumer/show/backend/client";
import { SHOW_PRICE } from "@phading/price_config";
import { resolvePriceOfMonth } from "@phading/price_config/resolver";
import { ProcessMonthlyMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/consumer/show/backend/handler";
import {
  ProcessMonthlyMeterReadingRequestBody,
  ProcessMonthlyMeterReadingResponse,
} from "@phading/product_meter_service_interface/consumer/show/backend/interface";
import { newBadRequestError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

export class ProcessMonthlyMeterReadingHandler extends ProcessMonthlyMeterReadingHandlerInterface {
  public static create(): ProcessMonthlyMeterReadingHandler {
    return new ProcessMonthlyMeterReadingHandler(BIGTABLE, SERVICE_CLIENT);
  }

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
    let completed = rows[0].data["c"]["p"][0].value as string;
    let totalWatchTimeSec = rows[0].data["t"]["w"][0].value as number;
    if (!completed) {
      totalWatchTimeSec = await this.aggregateAndCheckPoint(
        body.rowKey,
        month,
        accountId,
      );
    }
    // Cleans up data rows.
    await this.bigtable.deleteRows(`t2#${month}#${accountId}`);
    await this.writeOutputRowsAndGenerateTransaction(
      month,
      accountId,
      totalWatchTimeSec,
    );
    // Marks the completion.
    await this.bigtable.row(body.rowKey).delete();
    return {};
  }

  private async aggregateAndCheckPoint(
    rowKey: string,
    month: string,
    accountId: string,
  ): Promise<number> {
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
    let totalWatchTimeSec = 0;
    for (let row of rows) {
      totalWatchTimeSec += row.data["t"]["w"][0].value;
    }
    await this.bigtable.insert([
      {
        key: rowKey,
        data: {
          t: {
            w: {
              value: totalWatchTimeSec,
            },
          },
          c: {
            p: {
              value: "1",
            },
          },
        },
      },
    ]);
    this.interruptAfterCheckPoint();
    return totalWatchTimeSec;
  }

  private async writeOutputRowsAndGenerateTransaction(
    month: string,
    accountId: string,
    totalWatchTimeSec: number,
  ): Promise<void> {
    let price = resolvePriceOfMonth(SHOW_PRICE, month);
    let totalAmount = Math.ceil(
      (totalWatchTimeSec * price.money.amount) / price.divideBy,
    );
    await Promise.all([
      this.bigtable.insert([
        {
          key: `f3#${accountId}#${month}`,
          data: {
            t: {
              [`${price.money.currency}c`]: {
                value: totalAmount,
              },
            },
          }
        },
      ]),
      generateBillingStatement(this.serviceClient, {
        accountId,
        month,
        items: [
          {
            price,
            quantity: totalWatchTimeSec,
            subTotal: {
              amount: totalAmount,
              currency: price.money.currency,
            },
          },
        ],
        total: {
          amount: totalAmount,
          currency: price.money.currency,
        },
      }),
    ]);
  }
}
