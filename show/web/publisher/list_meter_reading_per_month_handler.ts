import { BIGTABLE } from "../../../common/bigtable";
import {
  getMonthDifference,
  toMonthISOString,
} from "../../../common/date_helper";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { MAX_MONTH_RANGE } from "@phading/constants/meter";
import { ListMeterReadingsPerMonthHandlerInterface } from "@phading/product_meter_service_interface/show/web/publisher/handler";
import {
  ListMeterReadingsPerMonthRequestBody,
  ListMeterReadingsPerMonthResponse,
} from "@phading/product_meter_service_interface/show/web/publisher/interface";
import { MeterReadingPerMonth } from "@phading/product_meter_service_interface/show/web/publisher/meter_reading";
import { exchangeSessionAndCheckCapability } from "@phading/user_session_service_interface/node/client";
import { newBadRequestError, newUnauthorizedError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

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
    let startMonth = new Date(body.startMonth);
    if (isNaN(startMonth.valueOf())) {
      throw newBadRequestError(`"startMonth" is not a valid date.`);
    }
    let endMonth = new Date(body.endMonth);
    if (isNaN(endMonth.valueOf())) {
      throw newBadRequestError(`"endMonth" is not a valid date.`);
    }
    if (startMonth >= endMonth) {
      throw newBadRequestError(`"startMonth" must be smaller than "endMonth".`);
    }
    if (getMonthDifference(startMonth, endMonth) > MAX_MONTH_RANGE) {
      throw newBadRequestError(
        `The range between "startMonth" and "endMonth" is too large.`,
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
        `Account ${accountId} not allowed to list meter reading per month.`,
      );
    }

    let [rows] = await this.bigtable.getRows({
      start: `f4#${accountId}#${toMonthISOString(startMonth)}`,
      end: `f4#${accountId}#${toMonthISOString(endMonth)}`,
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
          watchTimeSecGraded: row.data["t"]["ws"]
            ? row.data["t"]["ws"][0].value
            : undefined,
          transmittedMb: row.data["t"]["nm"]
            ? row.data["t"]["nm"][0].value
            : undefined,
          uploadedMb: row.data["t"]["um"]
            ? row.data["t"]["um"][0].value
            : undefined,
          storageMbh: row.data["t"]["smh"]
            ? row.data["t"]["smh"][0].value
            : undefined,
        };
      },
    );
    return {
      readings,
    };
  }
}
