import { BIGTABLE } from "../../../common/bigtable";
import { incrementColumn } from "../../../common/bigtable_data_helper";
import {
  CACHE_SIZE_OF_GET_SEASON_PUBLISHER_AND_GRADE,
  CACHE_SIZE_OF_GET_VIDEO_DURATION_AND_SIZE,
} from "../../../common/params";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { ProcessDailyMeterReadingHandlerInterface } from "@phading/product_meter_service_interface/show/backend/consumer/handler";
import {
  ProcessDailyMeterReadingRequestBody,
  ProcessDailyMeterReadingResponse,
} from "@phading/product_meter_service_interface/show/backend/consumer/interface";
import {
  getSeasonPublisherAndGrade,
  getVideoDurationAndSize,
} from "@phading/product_service_interface/show/backend/client";
import {
  GetSeasonPublisherAndGradeResponse,
  GetVideoDurationAndSizeResponse,
} from "@phading/product_service_interface/show/backend/interface";
import { HttpError, StatusCode, newBadRequestError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { LRUCache } from "lru-cache";

export class ProcessDailyMeterReadingHandler extends ProcessDailyMeterReadingHandlerInterface {
  public static create(): ProcessDailyMeterReadingHandler {
    return new ProcessDailyMeterReadingHandler(BIGTABLE, SERVICE_CLIENT);
  }

  private static ONE_KB_IN_B = 1024;
  private seasonCache: LRUCache<
    string,
    Promise<GetSeasonPublisherAndGradeResponse>
  >;
  private episodeCache: LRUCache<
    string,
    Promise<GetVideoDurationAndSizeResponse>
  >;

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
  ) {
    super();
    this.seasonCache = new LRUCache({
      max: CACHE_SIZE_OF_GET_SEASON_PUBLISHER_AND_GRADE,
    });
    this.episodeCache = new LRUCache({
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
    // rowKey should be q1#${date}#${consumerId}
    let queueExists = (await this.bigtable.row(body.rowKey).exists())[0];
    if (!queueExists) {
      console.log(
        `${loggingPrefix} row ${body.rowKey} is not found because it has been processed.`,
      );
      return {};
    }

    let [_, todayString, accountId] = body.rowKey.split("#");
    let [row] = await this.bigtable.row(`d1#${todayString}#${accountId}`).get({
      filter: {
        column: {
          cellLimit: 1,
        },
      },
    });
    let columns = row.data["w"];
    await this.writeOutputRows(loggingPrefix, todayString, accountId, columns);
    // Queue is completed.
    await this.bigtable.row(body.rowKey).delete();
    return {};
  }

  private async writeOutputRows(
    loggingPrefix: string,
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
          loggingPrefix,
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
        key: `d2#${year}-${month}#${accountId}#${day}`,
        data: consumerMonthData,
      },
      {
        key: `q2#${year}-${month}#${accountId}`,
        data: {
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
          key: `d3#${todayString}#${publisherId}#${accountId}`,
          data,
        },
        {
          key: `q3#${todayString}#${publisherId}`,
          data: {
            c: {
              r: {
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
    loggingPrefix: string,
    seasonIdAndEpisodeId: string,
    watchTimeMs: number,
    date: string,
    consumerData: any,
    consumerMonthData: any,
    publishers: Map<string, any>,
  ): Promise<void> {
    let [seasonId, episodeId] = seasonIdAndEpisodeId.split("#");
    let seasonAndDate = `${seasonId}#${date}`;
    let seasonResponsePromise = this.seasonCache.get(seasonAndDate);
    if (!seasonResponsePromise) {
      seasonResponsePromise = getSeasonPublisherAndGrade(this.serviceClient, {
        seasonId,
        date,
      });
      this.seasonCache.set(seasonAndDate, seasonResponsePromise);
    }
    let seasonResponse: GetSeasonPublisherAndGradeResponse;
    try {
      seasonResponse = await seasonResponsePromise;
    } catch (e) {
      if (e instanceof HttpError && e.status === StatusCode.NotFound) {
        console.log(
          `${loggingPrefix} season ${seasonId} is not found. Might be a bad input from Sync RC. Ignore it.`,
        );
        return;
      } else {
        throw e;
      }
    }

    // Sequentailly fetch video after season in case there is bad data.
    let episodeResponsePromise = this.episodeCache.get(seasonIdAndEpisodeId);
    if (!episodeResponsePromise) {
      episodeResponsePromise = getVideoDurationAndSize(this.serviceClient, {
        seasonId,
        episodeId,
      });
    }
    let episodeResponse: GetVideoDurationAndSizeResponse;
    try {
      episodeResponse = await episodeResponsePromise;
    } catch (e) {
      if (e instanceof HttpError && e.status === StatusCode.NotFound) {
        console.log(
          `${loggingPrefix} season ${seasonId} episode ${episodeId} is not found. Might be a bad input from Sync RC. Ignore it.`,
        );
        return;
      } else {
        throw e;
      }
    }
    let transmittedKbs = Math.ceil(
      ((episodeResponse.videoSize / episodeResponse.videoDurationSec) *
        (watchTimeMs / 1000)) /
        ProcessDailyMeterReadingHandler.ONE_KB_IN_B,
    );

    let watchTimeSec = Math.ceil(watchTimeMs / 1000);
    let watchTimeSecGraded = Math.ceil(
      (watchTimeMs * seasonResponse.grade) / 1000,
    );

    incrementColumn(consumerData, "w", seasonId, watchTimeSec);
    incrementColumn(consumerData, "a", seasonId, watchTimeSecGraded);
    incrementColumn(consumerData, "t", `w`, watchTimeSecGraded);
    incrementColumn(consumerMonthData, "t", `w`, watchTimeSecGraded);

    let publisherData = publishers.get(seasonResponse.publisherId);
    if (!publisherData) {
      publisherData = {
        w: {},
        a: {},
      };
      publishers.set(seasonResponse.publisherId, publisherData);
    }
    incrementColumn(publisherData, "w", seasonId, watchTimeSec);
    incrementColumn(publisherData, "a", seasonId, watchTimeSecGraded);
    incrementColumn(publisherData, "t", "kb", transmittedKbs);
  }
}
