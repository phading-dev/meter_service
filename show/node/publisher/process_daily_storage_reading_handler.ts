import { BIGTABLE } from "../../../common/bigtable";
import { incrementColumn } from "../../../common/bigtable_data_helper";
import { ENV_VARS } from "../../../env_vars";
import { Table } from "@google-cloud/bigtable";
import { ProcessDailyStorageReadingHandlerInterface } from "@phading/meter_service_interface/show/node/publisher/handler";
import {
  ProcessDailyStorageReadingRequestBody,
  ProcessDailyStorageReadingResponse,
} from "@phading/meter_service_interface/show/node/publisher/interface";
import { newBadRequestError } from "@selfage/http_error";
import { TzDate } from "@selfage/tz_date";

export class ProcessDailyStorageReadingHandler extends ProcessDailyStorageReadingHandlerInterface {
  public static create(): ProcessDailyStorageReadingHandler {
    return new ProcessDailyStorageReadingHandler(BIGTABLE);
  }

  private static ONE_MB_IN_B = 1024 * 1024;
  private static ONE_KB_IN_B = 1024;

  public constructor(private bigtable: Table) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessDailyStorageReadingRequestBody,
  ): Promise<ProcessDailyStorageReadingResponse> {
    if (!body.rowKey) {
      throw newBadRequestError(`"rowKey" is required.`);
    }
    // rowKey should be t6#{date}#${publisherId}
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

    let [_, date, accountId] = body.rowKey.split("#");
    let [rows] = await this.bigtable.getRows({
      keys: [`d6#${date}#${accountId}`],
      filter: {
        column: {
          cellLimit: 1,
        },
      },
    });
    if (rows.length > 0) {
      await this.aggregateAndWriteOutputRows(
        loggingPrefix,
        date,
        accountId,
        rows[0].data,
      );
    }
    // Task is completed.
    await this.bigtable.row(body.rowKey).delete();
    return {};
  }

  private async aggregateAndWriteOutputRows(
    loggingPrefix: string,
    date: string,
    accountId: string,
    inputData: any,
  ): Promise<void> {
    let carryOverData: any = {};
    let aggregationData: any = {};
    let nextDay = TzDate.fromLocalDateString(
      date,
      ENV_VARS.timezoneNegativeOffset,
    ).addDays(1);
    let bytes: number;
    let endTimeMs: number;
    if (inputData["s"]) {
      Object.entries(inputData["s"]).forEach(([nameAndKind, cells]) => {
        let [name, category] = nameAndKind.split("#");
        if (category === "b") {
          bytes = (cells as any)[0].value;
        } else if (category === "e") {
          endTimeMs = (cells as any)[0].value;
        } else if (category === "s") {
          let startTimeMs = (cells as any)[0].value;
          let nextDayStartTimeMs = nextDay.toTimestampMs();
          let mbMin = Math.ceil(
            (bytes / ProcessDailyStorageReadingHandler.ONE_MB_IN_B) *
              (((endTimeMs ?? nextDayStartTimeMs) - startTimeMs) / 1000 / 60),
          );
          incrementColumn(aggregationData, "t", "smm", mbMin);
          if (!endTimeMs) {
            // Use incrementColumn to set column.
            incrementColumn(carryOverData, "s", `${name}#b`, bytes);
            incrementColumn(
              carryOverData,
              "s",
              `${name}#s`,
              nextDayStartTimeMs,
            );
          }
          bytes = undefined;
          endTimeMs = undefined;
        }
      });
    }
    if (inputData["u"]) {
      Object.entries(inputData["u"]).forEach(([name, cells]) => {
        let bytes = (cells as any)[0].value;
        incrementColumn(
          aggregationData,
          "t",
          "uk",
          Math.ceil(bytes / ProcessDailyStorageReadingHandler.ONE_KB_IN_B),
        );
      });
    }

    let [year, month, day] = date.split("-");
    let entries = new Array<any>();
    entries.push(
      {
        key: `f3#${accountId}#${date}`,
        data: aggregationData,
      },
      {
        key: `d5#${year}-${month}#${accountId}#${day}`,
        data: aggregationData,
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
    );
    if (carryOverData["s"]) {
      let nextDayStr = nextDay.toLocalDateISOString();
      entries.push(
        {
          key: `d6#${nextDayStr}#${accountId}`,
          data: carryOverData,
        },
        {
          key: `t6#${nextDayStr}#${accountId}`,
          data: {
            c: {
              p: {
                value: "",
              },
            },
          },
        },
      );
    }
    await this.bigtable.insert(entries);
  }
}
