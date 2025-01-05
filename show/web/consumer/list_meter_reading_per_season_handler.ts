import { BIGTABLE } from "../../../common/bigtable";
import {
  toDateISOString,
  toDateUtc,
  toYesterday,
} from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { ListMeterReadingPerSeasonHandlerInterface } from "@phading/product_meter_service_interface/show/web/consumer/handler";
import {
  ListMeterReadingPerSeasonRequestBody,
  ListMeterReadingPerSeasonResponse,
} from "@phading/product_meter_service_interface/show/web/consumer/interface";
import { MeterReadingPerSeason } from "@phading/product_meter_service_interface/show/web/consumer/meter_reading";
import { exchangeSessionAndCheckCapability } from "@phading/user_session_service_interface/node/client";
import { newBadRequestError, newUnauthorizedError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

export class ListMeterReadingPerSeasonHandler extends ListMeterReadingPerSeasonHandlerInterface {
  public static create(): ListMeterReadingPerSeasonHandler {
    return new ListMeterReadingPerSeasonHandler(
      BIGTABLE,
      SERVICE_CLIENT,
      () => new Date(),
    );
  }

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
    private getNowDate: () => Date,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ListMeterReadingPerSeasonRequestBody,
    sessionStr: string,
  ): Promise<ListMeterReadingPerSeasonResponse> {
    let date = body.date
      ? toDateUtc(body.date)
      : toYesterday(this.getNowDate());
    if (isNaN(date.valueOf())) {
      throw newBadRequestError(`"date" is not a valid date.`);
    }
    let { accountId, canConsumeShows } =
      await exchangeSessionAndCheckCapability(this.serviceClient, {
        signedSession: sessionStr,
        checkCanConsumeShows: true,
      });
    if (!canConsumeShows) {
      throw newUnauthorizedError(
        `Account ${accountId} not allowed to list meter reading per season.`,
      );
    }

    let dateString = toDateISOString(date);
    let [rows] = await this.bigtable.getRows({
      keys: [`f1#${accountId}#${dateString}`],
      filter: [
        {
          family: /^[a|w]$/,
        },
        {
          column: {
            cellLimit: 1,
          },
        },
      ],
    });
    if (rows.length === 0) {
      return {
        readings: [],
      };
    }
    let data = rows[0].data;
    let readings = new Array<MeterReadingPerSeason>();
    for (let seasonId in data["a"]) {
      readings.push({
        seasonId,
        watchTimeSec: data["w"][seasonId][0].value,
        watchTimeSecGraded: data["a"][seasonId][0].value,
      });
    }
    return {
      readings,
    };
  }
}
