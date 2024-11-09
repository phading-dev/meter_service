import { BIGTABLE } from "../../../common/bigtable";
import { toDateISOString, toYesterday } from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { ListMeterReadingPerSeasonHandlerInterface } from "@phading/product_meter_service_interface/show/frontend/publisher/handler";
import {
  ListMeterReadingPerSeasonRequestBody,
  ListMeterReadingPerSeasonResponse,
} from "@phading/product_meter_service_interface/show/frontend/publisher/interface";
import { MeterReadingPerSeason } from "@phading/product_meter_service_interface/show/frontend/publisher/meter_reading";
import { getSeasonName } from "@phading/product_service_interface/show/backend/client";
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
    let date = body.date ? new Date(body.date) : toYesterday(this.getNowDate());
    if (isNaN(date.valueOf())) {
      throw newBadRequestError(`"date" is not a valid date.`);
    }
    let { accountId, canPublishShows } =
      await exchangeSessionAndCheckCapability(this.serviceClient, {
        signedSession: sessionStr,
        checkCanPublishShows: true,
      });
    if (!canPublishShows) {
      throw newUnauthorizedError(
        `Account ${accountId} not allowed to list meter reading per season.`,
      );
    }

    let dateString = toDateISOString(date);
    let [rows] = await this.bigtable.getRows({
      keys: [`f2#${accountId}#${dateString}`],
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

    let readingsPromises = new Array<Promise<MeterReadingPerSeason>>();
    for (let seasonId in rows[0].data["a"]) {
      readingsPromises.push(
        this.getSeasonNameAndReading(
          seasonId,
          rows[0].data["w"][seasonId][0].value,
          rows[0].data["a"][seasonId][0].value,
        ),
      );
    }
    return {
      readings: await Promise.all(readingsPromises),
    };
  }

  private async getSeasonNameAndReading(
    seasonId: string,
    watchTimeSec: number,
    watchTimeSecGraded: number,
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
      watchTimeSecGraded,
    };
  }
}
