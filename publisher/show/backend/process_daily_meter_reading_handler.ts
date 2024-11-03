import { BIGTABLE } from "../../../common/bigtable";
import {
  incrementColumn,
  normalizeData,
} from "../../../common/bigtable_data_helper";
import {
  BATCH_SIZE_OF_DAILY_PROCESSING_CONUMSERS_FOR_ONE_PUBLISHER,
  CACHE_SIZE_OF_GET_VIDEO_DURATION_AND_SIZE,
} from "../../../common/params";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { ProcessDailyMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/publisher/show/backend/handler";
import {
  ProcessDailyMeterReadingRequestBody,
  ProcessDailyMeterReadingResponse,
} from "@phading/product_meter_service_interface/publisher/show/backend/interface";
import { getVideoDurationAndSize } from "@phading/product_service_interface/publisher/show/backend/client";
import { GetVideoDurationAndSizeResponse } from "@phading/product_service_interface/publisher/show/backend/interface";
import { newBadRequestError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { LRUCache } from "lru-cache";

export class ProcessDailyMeterReadingHandler extends ProcessDailyMeterReadingHandlerInterface {
  public static create(): ProcessDailyMeterReadingHandler {
    return new ProcessDailyMeterReadingHandler(
      BATCH_SIZE_OF_DAILY_PROCESSING_CONUMSERS_FOR_ONE_PUBLISHER,
      BIGTABLE,
      SERVICE_CLIENT,
    );
  }

  private static ONE_KB_IN_B = 1024;
  private cache: LRUCache<string, Promise<GetVideoDurationAndSizeResponse>>;

  public constructor(
    private batchSize: number,
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
    private interruptAggregation: () => void = () => {},
    private interruptFinalWrite: () => void = () => {},
  ) {
    super();
    this.cache = new LRUCache({
      max: CACHE_SIZE_OF_GET_VIDEO_DURATION_AND_SIZE,
    });
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
        let watchTimeSec = row.data["a"][seasonId][0].value;
        incrementColumn(data, "a", seasonId, watchTimeSec);
        incrementColumn(data, "t", "w", watchTimeSec);
      }
    }
    let kbsPromises = new Array<Promise<number>>();
    for (let row of rows) {
      for (let seasonIdAndEpisodeId in row.data["w"]) {
        kbsPromises.push(
          this.getTransimittedKbs(
            seasonIdAndEpisodeId,
            row.data["w"][seasonIdAndEpisodeId][0].value,
          ),
        );
      }
    }
    (await Promise.all(kbsPromises)).forEach((kbs) =>
      incrementColumn(data, "t", "kb", kbs),
    );
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

  private async getTransimittedKbs(
    seasonIdAndEpisodeId: string,
    watchTimeMs: number,
  ): Promise<number> {
    let [seasonId, episodeId] = seasonIdAndEpisodeId.split("#");
    let responsePromise = this.cache.get(seasonIdAndEpisodeId);
    if (!responsePromise) {
      responsePromise = getVideoDurationAndSize(this.serviceClient, {
        seasonId,
        episodeId,
      });
    }
    let { videoDurationSec, videoSize } = await responsePromise;
    return Math.ceil(
      ((videoSize / videoDurationSec) * (watchTimeMs / 1000)) /
        ProcessDailyMeterReadingHandler.ONE_KB_IN_B,
    );
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
