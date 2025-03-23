import { BIGTABLE } from "../../../common/bigtable";
import { incrementColumn } from "../../../common/bigtable_data_helper";
import {
  CACHE_SIZE_OF_GET_SEASON_GRADE,
  CACHE_SIZE_OF_GET_SEASON_PUBLISHER,
} from "../../../common/constants";
import { SERVICE_CLIENT } from "../../../common/service_client";
import { Table } from "@google-cloud/bigtable";
import { ProcessDailyMeterReadingHandlerInterface } from "@phading/meter_service_interface/show/node/consumer/handler";
import {
  ProcessDailyMeterReadingRequestBody,
  ProcessDailyMeterReadingResponse,
} from "@phading/meter_service_interface/show/node/consumer/interface";
import {
  newGetSeasonGradeRequest,
  newGetSeasonPublisherRequest,
} from "@phading/product_service_interface/show/node/client";
import {
  GetSeasonGradeResponse,
  GetSeasonPublisherResponse,
} from "@phading/product_service_interface/show/node/interface";
import { HttpError, StatusCode, newBadRequestError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { LRUCache } from "lru-cache";

export class ProcessDailyMeterReadingHandler extends ProcessDailyMeterReadingHandlerInterface {
  public static create(): ProcessDailyMeterReadingHandler {
    return new ProcessDailyMeterReadingHandler(BIGTABLE, SERVICE_CLIENT);
  }

  private static ONE_KB_IN_B = 1024;
  private seasonGradeCache: LRUCache<string, Promise<GetSeasonGradeResponse>>;
  private seasonPublisherCache: LRUCache<
    string,
    Promise<GetSeasonPublisherResponse>
  >;

  public constructor(
    private bigtable: Table,
    private serviceClient: NodeServiceClient,
  ) {
    super();
    this.seasonGradeCache = new LRUCache({
      max: CACHE_SIZE_OF_GET_SEASON_GRADE,
    });
    this.seasonPublisherCache = new LRUCache({
      max: CACHE_SIZE_OF_GET_SEASON_PUBLISHER,
    });
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessDailyMeterReadingRequestBody,
  ): Promise<ProcessDailyMeterReadingResponse> {
    if (!body.rowKey) {
      throw newBadRequestError(`"rowKey" is required.`);
    }
    // rowKey should be t1#${date}#${consumerId}
    let taskExists = (await this.bigtable.row(body.rowKey).exists())[0];
    if (!taskExists) {
      console.log(
        `${loggingPrefix} row ${body.rowKey} is not found because it has been processed.`,
      );
      return {};
    }

    let [_, date, accountId] = body.rowKey.split("#");
    let [rows] = await this.bigtable.getRows({
      keys: [`d1#${date}#${accountId}`],
      filter: {
        column: {
          cellLimit: 1,
        },
      },
    });
    if (rows.length > 0) {
      await this.writeOutputRows(loggingPrefix, date, accountId, rows[0].data);
    }
    // Task is completed.
    await this.bigtable.row(body.rowKey).delete();
    return {};
  }

  private async writeOutputRows(
    loggingPrefix: string,
    date: string,
    accountId: string,
    inputData: any,
  ): Promise<void> {
    let consumerDayDate: any = {};
    let consumerMonthData: any = {};
    let publishers = new Map<string, any>();
    let columnProcessingPromises = Object.entries(inputData["w"]).map(
      ([seasonIdAndEpisodeIdAndKind, cells]) =>
        this.processColumn(
          loggingPrefix,
          seasonIdAndEpisodeIdAndKind,
          (cells as any)[0].value,
          date,
          consumerDayDate,
          consumerMonthData,
          publishers,
        ),
    );
    await Promise.all(columnProcessingPromises);

    let [year, month, day] = date.split("-");
    let entries = new Array<any>();
    if (Object.keys(consumerDayDate).length > 0) {
      entries.push(
        {
          key: `f1#${accountId}#${date}`,
          data: consumerDayDate,
        },
        {
          key: `d2#${year}-${month}#${accountId}#${day}`,
          data: consumerMonthData,
        },
        {
          key: `t2#${year}-${month}#${accountId}`,
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
    publishers.forEach((data, publisherId) => {
      entries.push(
        {
          key: `d3#${date}#${publisherId}#${accountId}`,
          data,
        },
        {
          key: `t3#${date}#${publisherId}`,
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
    if (entries.length > 0) {
      await this.bigtable.insert(entries);
    }
  }

  private async processColumn(
    loggingPrefix: string,
    seasonIdAndEpisodeIdAndKind: string,
    value: number,
    date: string,
    consumerDayDate: any,
    consumerMonthData: any,
    publishers: Map<string, any>,
  ): Promise<void> {
    let [seasonId, _, kind] = seasonIdAndEpisodeIdAndKind.split("#");
    if (kind === "w") {
      let seasonGradeResponse: GetSeasonGradeResponse;
      let seasonPublisherResponse: GetSeasonPublisherResponse;
      try {
        [seasonGradeResponse, seasonPublisherResponse] = await Promise.all([
          this.getSeasonGrade(seasonId, date),
          this.getSeasonPublisher(seasonId),
        ]);
      } catch (e) {
        if (e instanceof HttpError && e.status === StatusCode.NotFound) {
          console.log(
            `Season ${seasonId} is not found. Might be a bad input from Sync RC. Ignore it.`,
          );
          return;
        } else {
          throw e;
        }
      }

      let watchTimeMs = value;
      let watchTimeSec = Math.ceil(watchTimeMs / 1000);
      let watchTimeSecGraded = Math.ceil(
        (watchTimeMs * seasonGradeResponse.grade) / 1000,
      );
      incrementColumn(consumerDayDate, "w", seasonId, watchTimeSec);
      incrementColumn(consumerDayDate, "a", seasonId, watchTimeSecGraded);
      incrementColumn(consumerDayDate, "t", `ws`, watchTimeSecGraded);
      incrementColumn(consumerMonthData, "t", `ws`, watchTimeSecGraded);

      let publisherData = publishers.get(seasonPublisherResponse.publisherId);
      if (!publisherData) {
        publisherData = {
          w: {},
          a: {},
        };
        publishers.set(seasonPublisherResponse.publisherId, publisherData);
      }
      incrementColumn(publisherData, "w", seasonId, watchTimeSec);
      incrementColumn(publisherData, "a", seasonId, watchTimeSecGraded);
    } else if (kind === "n") {
      let seasonPublisherResponse: GetSeasonPublisherResponse;
      try {
        seasonPublisherResponse = await this.getSeasonPublisher(seasonId);
      } catch (e) {
        if (e instanceof HttpError && e.status === StatusCode.NotFound) {
          console.log(
            `Season ${seasonId} is not found. Might be a bad input from Sync RC. Ignore it.`,
          );
          return;
        } else {
          throw e;
        }
      }
      let publisherData = publishers.get(seasonPublisherResponse.publisherId);
      if (!publisherData) {
        publisherData = {
          w: {},
          a: {},
        };
        publishers.set(seasonPublisherResponse.publisherId, publisherData);
      }

      let networkTransmittedBytes = value;
      incrementColumn(
        publisherData,
        "t",
        "nk",
        Math.ceil(
          networkTransmittedBytes / ProcessDailyMeterReadingHandler.ONE_KB_IN_B,
        ),
      );
    }
  }

  private async getSeasonGrade(
    seasonId: string,
    date: string,
  ): Promise<GetSeasonGradeResponse> {
    let seasonAndDate = `${seasonId}#${date}`;
    let responesPromise = this.seasonGradeCache.get(seasonAndDate);
    if (!responesPromise) {
      responesPromise = this.serviceClient.send(
        newGetSeasonGradeRequest({
          seasonId,
          date,
        }),
      );
      this.seasonGradeCache.set(seasonAndDate, responesPromise);
    }
    return await responesPromise;
  }

  private async getSeasonPublisher(
    seasonId: string,
  ): Promise<GetSeasonPublisherResponse> {
    let responesPromise = this.seasonPublisherCache.get(seasonId);
    if (!responesPromise) {
      responesPromise = this.serviceClient.send(
        newGetSeasonPublisherRequest({
          seasonId,
        }),
      );
      this.seasonPublisherCache.set(seasonId, responesPromise);
    }
    return await responesPromise;
  }
}
