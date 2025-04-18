import { BIGTABLE } from "../../../common/bigtable";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { ENV_VARS } from "../../../env_vars";
import { Table } from "@google-cloud/bigtable";
import { MAX_DAY_RANGE } from "@phading/constants/meter";
import { ListMeterReadingsPerDayHandlerInterface } from "@phading/meter_service_interface/show/web/consumer/handler";
import {
  ListMeterReadingsPerDayRequestBody,
  ListMeterReadingsPerDayResponse,
} from "@phading/meter_service_interface/show/web/consumer/interface";
import { MeterReadingPerDay } from "@phading/meter_service_interface/show/web/consumer/meter_reading";
import { newFetchSessionAndCheckCapabilityRequest } from "@phading/user_session_service_interface/node/client";
import { newBadRequestError, newUnauthorizedError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { TzDate } from "@selfage/tz_date";

export class ListMeterReadingsPerDayHandler extends ListMeterReadingsPerDayHandlerInterface {
  public static create(): ListMeterReadingsPerDayHandler {
    return new ListMeterReadingsPerDayHandler(BIGTABLE, SERVICE_CLIENT);
  }

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ListMeterReadingsPerDayRequestBody,
    sessionStr: string,
  ): Promise<ListMeterReadingsPerDayResponse> {
    if (!body.startDate) {
      throw newBadRequestError(`"startDate" is required.`);
    }
    if (!body.endDate) {
      throw newBadRequestError(`"endDate" is required.`);
    }
    let startDate = TzDate.fromLocalDateString(
      body.startDate,
      ENV_VARS.timezoneNegativeOffset,
    );
    if (isNaN(startDate.toTimestampMs())) {
      throw newBadRequestError(`"startDate" is not a valid date.`);
    }
    let endDate = TzDate.fromLocalDateString(
      body.endDate,
      ENV_VARS.timezoneNegativeOffset,
    );
    if (isNaN(endDate.toTimestampMs())) {
      throw newBadRequestError(`"endDate" is not a valid date.`);
    }
    if (startDate.toTimestampMs() > endDate.toTimestampMs()) {
      throw newBadRequestError(`"startDate" must be smaller than "endDate".`);
    }
    if (endDate.minusDateInDays(startDate) + 1 > MAX_DAY_RANGE) {
      throw newBadRequestError(
        `The range between "startDate" and "endDate" is too large.`,
      );
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
        `Account ${accountId} not allowed to list meter reading per day.`,
      );
    }

    let [rows] = await this.bigtable.getRows({
      start: `f1#${accountId}#${startDate.toLocalDateISOString()}`,
      end: `f1#${accountId}#${endDate.toLocalDateISOString()}`,
      filter: [
        {
          family: /^t$/,
        },
        {
          column: {
            cellLimit: 1,
          },
        },
      ],
    });
    let readings: Array<MeterReadingPerDay> = rows.map(
      (row): MeterReadingPerDay => {
        return {
          date: row.id.split("#")[2],
          watchTimeSecGraded: row.data["t"]["ws"][0].value,
        };
      },
    );
    return {
      readings,
    };
  }
}
