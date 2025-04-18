import { BIGTABLE } from "../../../common/bigtable";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { ENV_VARS } from "../../../env_vars";
import { Table } from "@google-cloud/bigtable";
import { MAX_DAY_RANGE } from "@phading/constants/meter";
import { ListMeterReadingsPerDayHandlerInterface } from "@phading/meter_service_interface/show/web/publisher/handler";
import {
  ListMeterReadingsPerDayRequestBody,
  ListMeterReadingsPerDayResponse,
} from "@phading/meter_service_interface/show/web/publisher/interface";
import { MeterReadingPerDay } from "@phading/meter_service_interface/show/web/publisher/meter_reading";
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
    if (startDate.toTimestampMs() >= endDate.toTimestampMs()) {
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
          checkCanPublish: true,
        },
      }),
    );
    if (!capabilities.canPublish) {
      throw newUnauthorizedError(
        `Account ${accountId} not allowed to list meter reading per day.`,
      );
    }

    let [rows] = await this.bigtable.getRows({
      start: `f3#${accountId}#${startDate.toLocalDateISOString()}`,
      end: `f3#${accountId}#${endDate.toLocalDateISOString()}`,
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
          watchTimeSecGraded: row.data["t"]["ws"]
            ? row.data["t"]["ws"][0].value
            : undefined,
          transmittedKb: row.data["t"]["nk"]
            ? row.data["t"]["nk"][0].value
            : undefined,
          uploadedKb: row.data["t"]["uk"]
            ? row.data["t"]["uk"][0].value
            : undefined,
          storageMbm: row.data["t"]["smm"]
            ? row.data["t"]["smm"][0].value
            : undefined,
        };
      },
    );
    return {
      readings,
    };
  }
}
