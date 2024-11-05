import { BIGTABLE } from "../../../common/bigtable";
import {
  incrementColumn,
  normalizeData,
} from "../../../common/bigtable_data_helper";
import { BATCH_SIZE_OF_DAILY_PROCESSING_CONUMSERS_FOR_ONE_PUBLISHER } from "../../../common/params";
import { Table } from "@google-cloud/bigtable";
import { ProcessDailyMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/publisher/show/backend/handler";
import {
  ProcessDailyMeterReadingRequestBody,
  ProcessDailyMeterReadingResponse,
} from "@phading/product_meter_service_interface/publisher/show/backend/interface";
import { newBadRequestError } from "@selfage/http_error";

export class ProcessDailyMeterReadingHandler extends ProcessDailyMeterReadingHandlerInterface {
  public static create(): ProcessDailyMeterReadingHandler {
    return new ProcessDailyMeterReadingHandler(
      BATCH_SIZE_OF_DAILY_PROCESSING_CONUMSERS_FOR_ONE_PUBLISHER,
      BIGTABLE,
    );
  }

  public constructor(
    private batchSize: number,
    private bigtable: Table,
    private interruptAggregation: () => void = () => {},
    private interruptFinalWrite: () => void = () => {},
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
    let [rows] = await this.bigtable.getRows({
      keys: [body.rowKey],
      filter: {
        column: {
          cellLimit: 1,
        },
      },
    });
    if (rows.length === 0) {
      console.log(
        `${loggingPrefix} row ${body.rowKey} is not found maybe because it has been processed.`,
      );
      return {};
    }
    let data = normalizeData(rows[0].data);
    let [_, date, accountId] = body.rowKey.split("#");
    while (!data["c"]["p"].value) {
      await this.aggregateBatchAndCheckPoint(
        body.rowKey,
        date,
        accountId,
        this.batchSize,
        data,
      );
    }
    // Cleans up data rows.
    await this.bigtable.deleteRows(`t3#${date}#${accountId}`);
    await this.writeOutputRows(date, accountId, data);
    // Marks the completion.
    await this.bigtable.row(body.rowKey).delete();
    return {};
  }

  // Modifies `data` in place.
  private async aggregateBatchAndCheckPoint(
    rowKey: string,
    date: string,
    accountId: string,
    limit: number,
    data: any,
  ): Promise<void> {
    // `+` sign is larger than `#` sign, so it can mark the end of the range.
    let end = `t3#${date}#${accountId}+`;
    let cursor = data["c"]["r"].value;
    let start = cursor ? cursor + "0" : `t3#${date}#${accountId}`;
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
    let completed = newCursor ? "" : "1";
    data["c"] = {
      r: {
        value: newCursor,
      },
      p: {
        value: completed,
      },
    };
    await this.bigtable.insert({
      key: rowKey,
      data,
    });
    this.interruptAggregation();
  }

  private async writeOutputRows(
    date: string,
    accountId: string,
    data: any,
  ): Promise<void> {
    // cursor and completed columns are not needed in the final data.
    delete data["c"];
    let [year, month, day] = date.split("-");
    await this.bigtable.insert([
      {
        key: `f2#${accountId}#${date}`,
        data,
      },
      {
        key: `t5#${year}-${month}#${accountId}#${day}`,
        data: {
          t: {
            w: data["t"]["w"].value,
            kb: data["t"]["kb"].value,
          },
        },
      },
    ]);
    this.interruptFinalWrite();
  }
}
