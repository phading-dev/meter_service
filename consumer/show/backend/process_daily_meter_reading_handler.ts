import { BIGTABLE } from "../../../common/bigtable";
import { incrementColumn } from "../../../common/bigtable_data_helper";
import { CACHE_SIZE_OF_GET_SEASON_PUBLISHER_AND_GRADE } from "../../../common/params";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { ProcessDailyMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/consumer/show/backend/handler";
import {
  ProcessDailyMeterReadingRequestBody,
  ProcessDailyMeterReadingResponse,
} from "@phading/product_meter_service_interface/consumer/show/backend/interface";
import { getSeasonPublisherAndGrade } from "@phading/product_service_interface/consumer/show/backend/client";
import { GetSeasonPublisherAndGradeResponse } from "@phading/product_service_interface/consumer/show/backend/interface";
import { newBadRequestError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { LRUCache } from "lru-cache";

export class ProcessDailyMeterReadingHandler extends ProcessDailyMeterReadingHandlerInterface {
  public static create(): ProcessDailyMeterReadingHandler {
    return new ProcessDailyMeterReadingHandler(BIGTABLE, SERVICE_CLIENT);
  }

  private cache: LRUCache<string, Promise<GetSeasonPublisherAndGradeResponse>>;

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
  ) {
    super();
    this.cache = new LRUCache({
      max: CACHE_SIZE_OF_GET_SEASON_PUBLISHER_AND_GRADE,
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

    let [_, todayString, accountId] = body.rowKey.split("#");
    let columns = rows[0].data["w"];
    await this.writeOutputRows(todayString, accountId, columns);
    // Marks the completion.
    await this.bigtable.row(body.rowKey).delete();
    return {};
  }

  private async writeOutputRows(
    todayString: string,
    accountId: string,
    columns: any,
  ): Promise<void> {
    let consumerData: any = {};
    let consumerMonthData: any = {};
    let publishers = new Map<string, any>();
    let columnProcessingPromises = new Array<Promise<void>>();
    for (let seasonIdAndEpisodeId in columns) {
      columnProcessingPromises.push(
        this.processColumn(
          seasonIdAndEpisodeId,
          columns[seasonIdAndEpisodeId][0].value,
          todayString,
          consumerData,
          consumerMonthData,
          publishers,
        ),
      );
    }
    await Promise.all(columnProcessingPromises);

    let [year, month, day] = todayString.split("-");
    let entries: Array<any> = [
      {
        key: `f1#${accountId}#${todayString}`,
        data: consumerData,
      },
      {
        key: `t2#${year}-${month}#${accountId}#${day}`,
        data: consumerMonthData,
      },
      {
        key: `t6#${year}-${month}#${accountId}`,
        data: {
          t: {
            w: {
              value: 0,
            },
          },
          c: {
            p: {
              value: "",
            },
          },
        },
      },
    ];
    publishers.forEach((data, publisherId) => {
      entries.push(
        {
          key: `t3#${todayString}#${publisherId}#${accountId}`,
          data,
        },
        {
          key: `t4#${todayString}#${publisherId}`,
          data: {
            t: {
              w: {
                value: 0,
              },
              b: {
                value: 0,
              },
            },
            c: {
              r: {
                value: "",
              },
              p: {
                value: "",
              },
            },
          },
        },
      );
    });
    await this.bigtable.insert(entries);
  }

  private async processColumn(
    seasonIdAndEpisodeId: string,
    watchTimeMs: number,
    date: string,
    consumerData: any,
    consumerMonthData: any,
    publishers: Map<string, any>,
  ): Promise<void> {
    let [seasonId] = seasonIdAndEpisodeId.split("#");
    let cacheKey = `${seasonId}#${date}`;
    let responsePromise = this.cache.get(cacheKey);
    if (!responsePromise) {
      responsePromise = getSeasonPublisherAndGrade(this.serviceClient, {
        seasonId,
        date,
      });
      this.cache.set(cacheKey, responsePromise);
    }
    let { publisherId, grade } = await responsePromise;
    let watchTimeSec = Math.ceil(watchTimeMs / 1000);
    let multipliedWatchTimeSec = Math.ceil((watchTimeMs * grade) / 1000);

    incrementColumn(consumerData, "w", seasonId, watchTimeSec);
    incrementColumn(consumerData, "t", `w`, multipliedWatchTimeSec);
    incrementColumn(consumerMonthData, "t", `w`, multipliedWatchTimeSec);

    let publisherData = publishers.get(publisherId);
    if (!publisherData) {
      publisherData = {
        w: {},
        a: {},
      };
      publishers.set(publisherId, publisherData);
    }
    publisherData["w"][seasonIdAndEpisodeId] = {
      value: watchTimeMs,
    };
    incrementColumn(publisherData, "a", seasonId, multipliedWatchTimeSec);
  }
}
