import { BIGTABLE } from "../../../common/bigtable";
import {
  toDateISOString,
  toDateUtc,
  toYesterday,
} from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { ListMeterReadingPerSeasonHandlerInterface } from "@phading/meter_service_interface/show/web/publisher/handler";
import {
  ListMeterReadingPerSeasonRequestBody,
  ListMeterReadingPerSeasonResponse,
} from "@phading/meter_service_interface/show/web/publisher/interface";
import { MeterReadingPerSeason } from "@phading/meter_service_interface/show/web/publisher/meter_reading";
import { newFetchSessionAndCheckCapabilityRequest } from "@phading/user_session_service_interface/node/client";
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
    let { accountId, capabilities } = await this.serviceClient.send(
      newFetchSessionAndCheckCapabilityRequest({
        signedSession: sessionStr,
        capabilitiesMask: {
          checkCanPublish: true,
        },
      }),
    );
    if (!capabilities.canPublish) {
      throw newUnauthorizedError(
        `Account ${accountId} not allowed to list meter reading per season.`,
      );
    }

    let dateString = toDateISOString(date);
    let [rows] = await this.bigtable.getRows({
      keys: [`f3#${accountId}#${dateString}`],
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
