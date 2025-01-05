import crypto = require("crypto");
import { BIGTABLE } from "../../../common/bigtable";
import {
  incrementColumn,
  normalizeData,
} from "../../../common/bigtable_data_helper";
import { BATCH_SIZE_OF_DAILY_PROCESSING_CONUMSERS_FOR_ONE_PUBLISHER } from "../../../common/params";
import { Table } from "@google-cloud/bigtable";
import { ProcessDailyWatchReadingHandlerInterface } from "@phading/product_meter_service_interface/show/node/publisher/handler";
import {
  ProcessDailyWatchReadingRequestBody,
  ProcessDailyWatchReadingResponse,
} from "@phading/product_meter_service_interface/show/node/publisher/interface";
import { newBadRequestError } from "@selfage/http_error";

export class ProcessDailyWatchReadingHandler extends ProcessDailyWatchReadingHandlerInterface {
  public static create(): ProcessDailyWatchReadingHandler {
    return new ProcessDailyWatchReadingHandler(
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
    body: ProcessDailyWatchReadingRequestBody,
  ): Promise<ProcessDailyWatchReadingResponse> {
    if (!body.rowKey) {
      throw newBadRequestError(`"rowKey" is required.`);
    }
    // rowKey should be t3#{date}#${publisherId} or t3#{date}#${publisherId}#${checkpointId}
    let [taskRows] = await this.bigtable.getRows({
      keys: [body.rowKey],
      filter: {
        column: {
          cellLimit: 1,
        },
      },
    });
    if (taskRows.length === 0) {
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
    let taskKey = body.rowKey;
    let cursor = taskRows[0].data["c"]["r"][0].value;
    while (taskKey) {
      [taskKey, cursor] = await this.aggregateBatchAndCheckPoint(
        taskKey,
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
    taskKey: string,
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
      if (row.data['w']) {
        Object.entries(row.data["w"]).forEach(([seasonId, cells]) => {
          incrementColumn(data, "w", seasonId, (cells as any)[0].value);
        });
        Object.entries(row.data["a"]).forEach(([seasonId, cells]) => {
          let watchTimeSecGraded = (cells as any)[0].value;
          incrementColumn(data, "a", seasonId, watchTimeSecGraded);
          incrementColumn(data, "t", "w", watchTimeSecGraded);
        });
      }
      if (row.data["t"] && row.data["t"]["n"]) {
        incrementColumn(data, "t", "n", row.data["t"]["n"][0].value);
      }
    }
    let newCursor =
      rows.length === limit ? rows[rows.length - 1].id : undefined;
    let newTaskKey: string;
    if (newCursor) {
      let checkpointId = this.generateUuid();
      newTaskKey = `t3#${date}#${accountId}#${checkpointId}`;
      await this.bigtable.insert([
        {
          key: newTaskKey,
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
      let monthData: any = {};
      if (data["t"]["w"]) {
        incrementColumn(monthData, "t", "w", data["t"]["w"].value);
      }
      if (data["t"]["n"]) {
        incrementColumn(monthData, "t", "n", data["t"]["n"].value);
      }
      await this.bigtable.insert([
        {
          key: `f3#${accountId}#${date}`,
          data,
        },
        {
          key: `d5#${year}-${month}#${accountId}#${day}`,
          data: monthData,
        },
        {
          key: `t5#${year}-${month}#${accountId}`,
          data: {
            c: {
              p: {
                value: "",
              },
            },
          },
        },
      ]);
    }
    // Task is completed.
    await this.bigtable.row(taskKey).delete();
    this.interruptAfterCheckPoint();
    return [newTaskKey, newCursor];
  }
}
