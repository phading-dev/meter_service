import { BIGTABLE } from "../../../common/bigtable";
import { getDayDifference, toDateISOString } from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { MAX_DAY_RANGE } from "@phading/constants/meter";
import { ListMeterReadingsPerDayHandlerInterface } from "@phading/product_meter_service_interface/show/web/publisher/handler";
import {
  ListMeterReadingsPerDayRequestBody,
  ListMeterReadingsPerDayResponse,
} from "@phading/product_meter_service_interface/show/web/publisher/interface";
import { MeterReadingPerDay } from "@phading/product_meter_service_interface/show/web/publisher/meter_reading";
import { exchangeSessionAndCheckCapability } from "@phading/user_session_service_interface/node/client";
import { newBadRequestError, newUnauthorizedError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

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
    let startDate = new Date(body.startDate);
    if (isNaN(startDate.valueOf())) {
      throw newBadRequestError(`"startDate" is not a valid date.`);
    }
    let endDate = new Date(body.endDate);
    if (isNaN(endDate.valueOf())) {
      throw newBadRequestError(`"endDate" is not a valid date.`);
    }
    if (startDate >= endDate) {
      throw newBadRequestError(`"startDate" must be smaller than "endDate".`);
    }
    if (getDayDifference(startDate, endDate) > MAX_DAY_RANGE) {
      throw newBadRequestError(
        `The range between "startDate" and "endDate" is too large.`,
      );
    }
    let { accountId, capabilities } = await exchangeSessionAndCheckCapability(
      this.serviceClient,
      {
        signedSession: sessionStr,
        capabilitiesMask: {
          checkCanPublishShows: true,
        },
      },
    );
    if (!capabilities.canPublishShows) {
      throw newUnauthorizedError(
        `Account ${accountId} not allowed to list meter reading per day.`,
      );
    }

    let [rows] = await this.bigtable.getRows({
      start: `f3#${accountId}#${toDateISOString(startDate)}`,
      end: `f3#${accountId}#${toDateISOString(endDate)}`,
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
