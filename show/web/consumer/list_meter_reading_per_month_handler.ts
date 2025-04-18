import { BIGTABLE } from "../../../common/bigtable";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { ENV_VARS } from "../../../env_vars";
import { Table } from "@google-cloud/bigtable";
import { MAX_MONTH_RANGE } from "@phading/constants/meter";
import { ListMeterReadingsPerMonthHandlerInterface } from "@phading/meter_service_interface/show/web/consumer/handler";
import {
  ListMeterReadingsPerMonthRequestBody,
  ListMeterReadingsPerMonthResponse,
} from "@phading/meter_service_interface/show/web/consumer/interface";
import { MeterReadingPerMonth } from "@phading/meter_service_interface/show/web/consumer/meter_reading";
import { newFetchSessionAndCheckCapabilityRequest } from "@phading/user_session_service_interface/node/client";
import { newBadRequestError, newUnauthorizedError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { TzDate } from "@selfage/tz_date";

export class ListMeterReadingsPerMonthHandler extends ListMeterReadingsPerMonthHandlerInterface {
  public static create(): ListMeterReadingsPerMonthHandler {
    return new ListMeterReadingsPerMonthHandler(BIGTABLE, SERVICE_CLIENT);
  }

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ListMeterReadingsPerMonthRequestBody,
    sessionStr: string,
  ): Promise<ListMeterReadingsPerMonthResponse> {
    if (!body.startMonth) {
      throw newBadRequestError(`"startMonth" is required.`);
    }
    if (!body.endMonth) {
      throw newBadRequestError(`"endMonth" is required.`);
    }
    let startMonth = TzDate.fromLocalDateString(
      body.startMonth,
      ENV_VARS.timezoneNegativeOffset,
    );
    if (isNaN(startMonth.toTimestampMs())) {
      throw newBadRequestError(`"startMonth" is not a valid date.`);
    }
    let endMonth = TzDate.fromLocalDateString(
      body.endMonth,
      ENV_VARS.timezoneNegativeOffset,
    );
    if (isNaN(endMonth.toTimestampMs())) {
      throw newBadRequestError(`"endMonth" is not a valid date.`);
    }
    if (startMonth.toTimestampMs() > endMonth.toTimestampMs()) {
      throw newBadRequestError(`"startMonth" must be smaller than "endMonth".`);
    }
    if (endMonth.minusDateInMonths(startMonth) + 1 > MAX_MONTH_RANGE) {
      throw newBadRequestError(
        `The range between "startMonth" and "endMonth" is too large.`,
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
        `Account ${accountId} not allowed to list meter reading per month.`,
      );
    }

    let [rows] = await this.bigtable.getRows({
      start: `f2#${accountId}#${startMonth.toLocalMonthISOString()}`,
      end: `f2#${accountId}#${endMonth.toLocalMonthISOString()}`,
      filter: {
        column: {
          cellLimit: 1,
        },
      },
    });
    let readings: Array<MeterReadingPerMonth> = rows.map(
      (row): MeterReadingPerMonth => {
        return {
          month: row.id.split("#")[2],
          watchTimeSecGraded: row.data["t"]["ws"][0].value,
        };
      },
    );
    return {
      readings,
    };
  }
}
