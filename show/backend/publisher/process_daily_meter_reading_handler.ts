import crypto = require("crypto");
import { BIGTABLE } from "../../../common/bigtable";
import {
  incrementColumn,
  normalizeData,
} from "../../../common/bigtable_data_helper";
import { BATCH_SIZE_OF_DAILY_PROCESSING_CONUMSERS_FOR_ONE_PUBLISHER } from "../../../common/params";
import { Table } from "@google-cloud/bigtable";
import { ProcessDailyMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/show/backend/publisher/handler";
import {
  ProcessDailyMeterReadingRequestBody,
  ProcessDailyMeterReadingResponse,
} from "@phading/product_meter_service_interface/show/backend/publisher/interface";
import { newBadRequestError } from "@selfage/http_error";

export class ProcessDailyMeterReadingHandler extends ProcessDailyMeterReadingHandlerInterface {
  public static create(): ProcessDailyMeterReadingHandler {
    return new ProcessDailyMeterReadingHandler(
      BATCH_SIZE_OF_DAILY_PROCESSING_CONUMSERS_FOR_ONE_PUBLISHER,
      BIGTABLE,
      () => crypto.randomUUID(),
    );
  }

  public interruptAfterCheckPoint: () => void = () => {};

  public constructor(
    private batchSize: number,
    private bigtable: Table,
    private generateUuid: () => string,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessDailyMeterReadingRequestBody,
  ): Promise<ProcessDailyMeterReadingResponse> {
    if (!body.rowKey) {
      throw newBadRequestError(`"rowKey" is required.`);
    }
    // rowKey should be q3#{date}#${publisherId} or q3#{date}#${publisherId}#${checkpointId}
    let [queueRows] = await this.bigtable.getRows({
      keys: [body.rowKey],
      filter: {
        column: {
          cellLimit: 1,
        },
      },
    });
    if (queueRows.length === 0) {
      console.log(
        `${loggingPrefix} row ${body.rowKey} is not found because it has been processed.`,
      );
      return {};
    }

    let [_, date, accountId, checkpointId] = body.rowKey.split("#");
    let data: any = {};
    if (checkpointId) {
      let [row] = await this.bigtable
        .row(`d4#${date}#${accountId}#${checkpointId}`)
        .get({
          filter: {
            column: {
              cellLimit: 1,
            },
          },
        });
      data = normalizeData(row.data);
    }
    let queueKey = body.rowKey;
    let cursor = queueRows[0].data["c"]["r"][0].value;
    while (queueKey) {
      [queueKey, cursor] = await this.aggregateBatchAndCheckPoint(
        queueKey,
        cursor,
        date,
        accountId,
        this.batchSize,
        data,
      );
    }
    return {};
  }

  // Modifies `data` in place.
  private async aggregateBatchAndCheckPoint(
    queueKey: string,
    cursor: string,
    date: string,
    accountId: string,
    limit: number,
    data: any,
  ): Promise<[string, string]> {
    // `+` sign is larger than `#` sign, so it can mark the end of the range.
    let end = `d3#${date}#${accountId}+`;
    let start = cursor ? cursor + "0" : `d3#${date}#${accountId}`;
    let [rows] = await this.bigtable.getRows({
      start,
      end,
      limit,
      filter: {
        column: {
          cellLimit: 1,
        },
      },
    });
    for (let row of rows) {
      for (let seasonId in row.data["a"]) {
        incrementColumn(data, "w", seasonId, row.data["w"][seasonId][0].value);
        let watchTimeSecGraded = row.data["a"][seasonId][0].value;
        incrementColumn(data, "a", seasonId, watchTimeSecGraded);
        incrementColumn(data, "t", "w", watchTimeSecGraded);
      }
      incrementColumn(data, "t", "kb", row.data["t"]["kb"][0].value);
    }
    let newCursor =
      rows.length === limit ? rows[rows.length - 1].id : undefined;
    let newQueueKey: string;
    if (newCursor) {
      let checkpointId = this.generateUuid();
      newQueueKey = `q3#${date}#${accountId}#${checkpointId}`;
      await this.bigtable.insert([
        {
          key: newQueueKey,
          data: {
            c: {
              r: {
                value: newCursor,
              },
            },
          },
        },
        {
          key: `d4#${date}#${accountId}#${checkpointId}`,
          data,
        },
      ]);
    } else {
      let [year, month, day] = date.split("-");
      await this.bigtable.insert([
        {
          key: `f3#${accountId}#${date}`,
          data,
        },
        {
          key: `d5#${year}-${month}#${accountId}#${day}`,
          data: {
            t: {
              w: data["t"]["w"].value,
              kb: data["t"]["kb"].value,
            },
          },
        },
      ]);
    }
    // Queue is completed.
    await this.bigtable.row(queueKey).delete();
    this.interruptAfterCheckPoint();
    return [newQueueKey, newCursor];
  }
}
