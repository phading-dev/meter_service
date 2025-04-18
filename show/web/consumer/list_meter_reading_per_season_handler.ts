import { BIGTABLE } from "../../../common/bigtable";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { ENV_VARS } from "../../../env_vars";
import { Table } from "@google-cloud/bigtable";
import { ListMeterReadingPerSeasonHandlerInterface } from "@phading/meter_service_interface/show/web/consumer/handler";
import {
  ListMeterReadingPerSeasonRequestBody,
  ListMeterReadingPerSeasonResponse,
} from "@phading/meter_service_interface/show/web/consumer/interface";
import { MeterReadingPerSeason } from "@phading/meter_service_interface/show/web/consumer/meter_reading";
import { newFetchSessionAndCheckCapabilityRequest } from "@phading/user_session_service_interface/node/client";
import { newBadRequestError, newUnauthorizedError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { TzDate } from "@selfage/tz_date";

export class ListMeterReadingPerSeasonHandler extends ListMeterReadingPerSeasonHandlerInterface {
  public static create(): ListMeterReadingPerSeasonHandler {
    return new ListMeterReadingPerSeasonHandler(BIGTABLE, SERVICE_CLIENT);
  }

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ListMeterReadingPerSeasonRequestBody,
    sessionStr: string,
  ): Promise<ListMeterReadingPerSeasonResponse> {
    if (!body.date) {
      throw newBadRequestError(`"date" is required.`);
    }
    let date = TzDate.fromLocalDateString(
      body.date,
      ENV_VARS.timezoneNegativeOffset,
    );
    if (isNaN(date.toTimestampMs())) {
      throw newBadRequestError(`"date" is not a valid date.`);
    }
    let { accountId, capabilities } = await this.serviceClient.send(
      newFetchSessionAndCheckCapabilityRequest({
        signedSession: sessionStr,
        capabilitiesMask: {
          checkCanConsume: true,
        },
      }),
    );
    if (!capabilities.canConsume) {
      throw newUnauthorizedError(
        `Account ${accountId} not allowed to list meter reading per season.`,
      );
    }

    let [rows] = await this.bigtable.getRows({
      keys: [`f1#${accountId}#${date.toLocalDateISOString()}`],
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
