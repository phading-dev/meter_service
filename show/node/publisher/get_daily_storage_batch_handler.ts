import { BIGTABLE } from "../../../common/bigtable";
import { BATCH_SIZE_OF_DAILY_STORAGE_PROCESSING_PUBLISHERS } from "../../../common/constants";
import { ENV_VARS } from "../../../env_vars";
import { Table } from "@google-cloud/bigtable";
import { GetDailyStorageBatchHandlerInterface } from "@phading/meter_service_interface/show/node/publisher/handler";
import {
  GetDailyStorageBatchRequestBody,
  GetDailyStorageBatchResponse,
} from "@phading/meter_service_interface/show/node/publisher/interface";
import { TzDate } from "@selfage/tz_date";

export class GetDailyStorageBatchHandler extends GetDailyStorageBatchHandlerInterface {
  public static create(): GetDailyStorageBatchHandler {
    return new GetDailyStorageBatchHandler(
      BATCH_SIZE_OF_DAILY_STORAGE_PROCESSING_PUBLISHERS,
      BIGTABLE,
      () => new Date(),
    );
  }

  public constructor(
    private batchSize: number,
    private bigtable: Table,
    private getNowDate: () => Date,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: GetDailyStorageBatchRequestBody,
  ): Promise<GetDailyStorageBatchResponse> {
    // Do not process today's data.
    let end = `t6#${TzDate.fromNewDate(
      this.getNowDate(),
      ENV_VARS.timezoneNegativeOffset,
    ).toLocalDateISOString()}`;
    // Add "0" to skip the start cursor.
    let start = body.cursor ? body.cursor + "0" : `t6#`;
    let [rows] = await this.bigtable.getRows({
      start,
      end,
      limit: this.batchSize,
      filter: [
        {
          row: {
            cellLimit: 1,
          },
        },
        {
          value: {
            strip: true,
          },
        },
      ],
    });
    let rowKeys = rows.map((row) => row.id);
    return {
      rowKeys,
      cursor:
        rowKeys.length === this.batchSize
          ? rowKeys[rowKeys.length - 1]
          : undefined,
    };
  }
}
