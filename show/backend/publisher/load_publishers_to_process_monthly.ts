import { BIGTABLE } from "../../../common/bigtable";
import {
  toMonthISOString,
  toMonthTimeMsWrtTimezone,
  toToday,
} from "../../../common/date_helper";
import {
  BATCH_SIZE_OF_LOADING_PUBLISHERS,
  COLD_START_MONTH_FOR_LOADING_PUBLISHERS,
} from "../../../common/params";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Row, Table } from "@google-cloud/bigtable";
import { LoadPublishersToProcessMonthlyHandlerInterface } from "@phading/product_meter_service_interface/show/backend/publisher/handler";
import {
  LoadPublishersToProcessMonthlyRequestBody,
  LoadPublishersToProcessMonthlyResponse,
} from "@phading/product_meter_service_interface/show/backend/publisher/interface";
import { AccountType } from "@phading/user_service_interface/account_type";
import { listAccounts } from "@phading/user_service_interface/backend/client";
import { NodeServiceClient } from "@selfage/node_service_client";

export class LoadPublishersToProcessMonthlyHandler extends LoadPublishersToProcessMonthlyHandlerInterface {
  public static create(): LoadPublishersToProcessMonthlyHandler {
    return new LoadPublishersToProcessMonthlyHandler(
      COLD_START_MONTH_FOR_LOADING_PUBLISHERS,
      BATCH_SIZE_OF_LOADING_PUBLISHERS,
      BIGTABLE,
      SERVICE_CLIENT,
      () => new Date(),
    );
  }

  public constructor(
    private coldStartMonth: string, // The month before it starts to process monthly publisher data.
    private batchSize: number,
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
    private getNowDate: () => Date,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: LoadPublishersToProcessMonthlyRequestBody,
  ): Promise<LoadPublishersToProcessMonthlyResponse> {
    let [rows] = await this.bigtable.getRows({
      keys: ["l1"],
      filter: {
        column: {
          cellLimit: 1,
        },
      },
    });
    let [month, timeMs] = this.getMonthAndTimeMs(rows);
    let thisMonthTimeMs = toMonthTimeMsWrtTimezone(
      toMonthISOString(toToday(this.getNowDate())),
    );
    while (true) {
      if (timeMs === 0) {
        let date = new Date(month);
        date.setUTCMonth(date.getUTCMonth() + 1);
        let nextMonth = toMonthISOString(date);
        date.setUTCMonth(date.getUTCMonth() + 1);
        let endOfNextMonthTimeMs = toMonthTimeMsWrtTimezone(
          toMonthISOString(date),
        );
        if (endOfNextMonthTimeMs > thisMonthTimeMs) {
          break;
        }

        month = nextMonth;
        timeMs = endOfNextMonthTimeMs;
      }

      let response = await listAccounts(this.serviceClient, {
        accountType: AccountType.PUBLISHER,
        limit: this.batchSize,
        createdTimeMsCursor: timeMs,
      });
      let entries: Array<any> = response.accountIds.map((accountId) => {
        return {
          key: `q5#${month}#${accountId}`,
          data: {
            c: {
              p: {
                value: "",
              },
            },
          },
        };
      });
      await this.bigtable.insert(entries);

      if (!response.createdTimeMsCursor) {
        timeMs = 0;
      } else {
        timeMs = response.createdTimeMsCursor;
      }
      // Not to be combined with the mutation above. It marks the completion of one batch.
      await this.bigtable.insert({
        key: `l1`,
        data: {
          c: {
            m: {
              value: month,
            },
            t: {
              value: timeMs,
            },
          },
        },
      });
    }
    return {};
  }

  private getMonthAndTimeMs(rows: Array<Row>): [string, number] {
    if (rows.length === 0) {
      // A cold start without `l1` row being populated.
      return [this.coldStartMonth, 0];
    }
    let month = rows[0].data["c"]["m"][0].value;
    let timeMs = rows[0].data["c"]["t"][0].value;
    return [month, timeMs];
  }
}
