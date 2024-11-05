import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toToday } from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { ListMeterReadingPerSeasonHandlerInterface } from "@phading/product_meter_service_interface/consumer/show/frontend/handler";
import {
  ListMeterReadingPerSeasonRequestBody,
  ListMeterReadingPerSeasonResponse,
} from "@phading/product_meter_service_interface/consumer/show/frontend/interface";
import { MeterReadingPerSeason } from "@phading/product_meter_service_interface/consumer/show/frontend/meter_reading";
import { getSeasonName } from "@phading/product_service_interface/consumer/show/backend/client";
import { exchangeSessionAndCheckCapability } from "@phading/user_session_service_interface/backend/client";
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
      ? new Date(body.date)
      : this.toYesterday(toToday(this.getNowDate()));
    if (isNaN(date.valueOf())) {
      throw newBadRequestError(`"date" is not a valid date.`);
    }
    let { userSession, canConsumeShows } =
      await exchangeSessionAndCheckCapability(this.serviceClient, {
        signedSession: sessionStr,
        checkCanConsumeShows: true,
      });
    if (!canConsumeShows) {
      throw newUnauthorizedError(
        `Account ${userSession.accountId} not allowed to list meter reading per season.`,
      );
    }

    let dateString = toDateISOString(date);
    let [rows] = await this.bigtable.getRows({
      keys: [`f1#${userSession.accountId}#${dateString}`],
      filter: [
        {
          family: /^a$/,
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

    let readingsPromises = new Array<Promise<MeterReadingPerSeason>>();
    for (let seasonId in rows[0].data["a"]) {
      readingsPromises.push(
        this.getSeasonNameAndReading(
          seasonId,
          rows[0].data["a"][seasonId][0].value,
        ),
      );
    }
    return {
      readings: await Promise.all(readingsPromises),
    };
  }

  private toYesterday(date: Date): Date {
    date.setUTCDate(date.getUTCDate() - 1);
    return date;
  }

  private async getSeasonNameAndReading(
    seasonId: string,
    watchTimeSec: number,
  ): Promise<MeterReadingPerSeason> {
    let response = await getSeasonName(this.serviceClient, {
      seasonId,
    });
    return {
      season: {
        seasonId,
        seasonName: response.seasonName,
      },
      watchTimeSec,
    };
  }
}
