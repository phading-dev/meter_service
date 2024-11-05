import { BIGTABLE } from "./common/bigtable";
import { GetDailyBatchHandler as ConsumerGetDailyBatchHandler } from "./consumer/show/backend/get_daily_batch_handler";
import { GetMonthlyBatchHandler as ConsumerGetMonthlyBatchHandler } from "./consumer/show/backend/get_monthly_batch_handler";
import { ProcessDailyMeterReadingHandler as ConsumerProcessDailyMeterReadingHandler } from "./consumer/show/backend/process_daily_meter_reading_handler";
import { ProcessMonthlyMeterReadingHandler as ConsumerProcessMonthlyMeterReadingHandler } from "./consumer/show/backend/process_monthly_meter_reading_handler";
import { ListMeterReadingsPerDayHandler as ConsumerListMeterReadingsPerDayHandler } from "./consumer/show/frontend/list_meter_reading_per_day_handler";
import { ListMeterReadingsPerMonthHandler as ConsumerListMeterReadingsPerMonthHandler } from "./consumer/show/frontend/list_meter_reading_per_month_handler";
import { ListMeterReadingPerSeasonHandler as ConsumerListMeterReadingPerSeasonHandler } from "./consumer/show/frontend/list_meter_reading_per_season_handler";
import { SyncMeterReadingHandler } from "./consumer/show/frontend/sync_meter_reading_handler";
import { GetDailyBatchHandler as PublisherGetDailyBatchHandler } from "./publisher/show/backend/get_daily_batch_handler";
import { GetMonthlyBatchHandler as PublisherGetMonthlyBatchHandler } from "./publisher/show/backend/get_monthly_batch_handler";
import { LoadPublishersToProcessMonthlyHandler } from "./publisher/show/backend/load_publishers_to_process_monthly";
import { ProcessDailyMeterReadingHandler as PublisherProcessDailyMeterReadingHandler } from "./publisher/show/backend/process_daily_meter_reading_handler";
import { ProcessMonthlyMeterReadingHandler as PublisherProcessMonthlyMeterReadingHandler } from "./publisher/show/backend/process_monthly_meter_reading_handler";
import {
  GENERATE_BILLING_STATEMENT,
  GENERATE_BILLING_STATEMENT_REQUEST_BODY,
  MeterType as ConsumerMeterType,
} from "@phading/commerce_service_interface/consumer/backend/interface";
import {
  GENERATE_EARNINGS_STATEMENT,
  GENERATE_EARNINGS_STATEMENT_REQUEST_BODY,
  MeterType as PublisherMeterType,
} from "@phading/commerce_service_interface/publisher/backend/interface";
import {
  LIST_METER_READINGS_PER_DAY_RESPONSE as CONSUMER_LIST_METER_READINGS_PER_DAY_RESPONSE,
  LIST_METER_READINGS_PER_MONTH_RESPONSE as CONSUMER_LIST_METER_READINGS_PER_MONTH_RESPONSE,
  LIST_METER_READING_PER_SEASON_RESPONSE as CONSUMER_LIST_METER_READING_PER_SEASON_RESPONSE,
} from "@phading/product_meter_service_interface/consumer/show/frontend/interface";
import {
  GET_SEASON_NAME,
  GET_SEASON_PUBLISHER_AND_GRADE,
  GET_VIDEO_DURATION_AND_SIZE,
  GetSeasonNameResponse,
  GetSeasonPublisherAndGradeResponse,
  GetVideoDurationAndSizeResponse,
} from "@phading/product_service_interface/consumer/show/backend/interface";
import {
  GET_STORAGE_METER_READING,
  GET_UPLOAD_METER_READING,
  GetStorageMeterReadingResponse,
  GetUploadMeterReadingResponse,
} from "@phading/product_service_interface/publisher/show/backend/interface";
import {
  LIST_ACCOUNTS,
  ListAccountsResponse,
} from "@phading/user_service_interface/third_person/backend/interface";
import {
  EXCHANGE_SESSION_AND_CHECK_CAPABILITY,
  ExchangeSessionAndCheckCapabilityResponse,
} from "@phading/user_session_service_interface/backend/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat, eq, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ProcessingIntegrationTest",
  cases: [
    {
      name: "ProcessOneRowE2E",
      execute: async () => {
        // Prepare
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === EXCHANGE_SESSION_AND_CHECK_CAPABILITY) {
              return {
                userSession: {
                  accountId: "consumer1",
                },
                canConsumeShows: true,
              } as ExchangeSessionAndCheckCapabilityResponse;
            } else if (request.descriptor === GET_SEASON_NAME) {
              return {
                seasonName: "name1",
              } as GetSeasonNameResponse;
            } else if (request.descriptor === GET_SEASON_PUBLISHER_AND_GRADE) {
              return {
                publisherId: "publisher1",
                grade: 5,
              } as GetSeasonPublisherAndGradeResponse;
            } else if (request.descriptor === GET_VIDEO_DURATION_AND_SIZE) {
              return {
                videoSize: 36000,
                videoDurationSec: 60,
              } as GetVideoDurationAndSizeResponse;
            } else if (request.descriptor === LIST_ACCOUNTS) {
              return {
                accountIds: ["publisher1"],
              } as ListAccountsResponse;
            } else if (request.descriptor === GET_STORAGE_METER_READING) {
              return {
                mbh: 132,
              } as GetStorageMeterReadingResponse;
            } else if (request.descriptor === GET_UPLOAD_METER_READING) {
              return {
                mb: 332,
              } as GetUploadMeterReadingResponse;
            } else if (
              request.descriptor === GENERATE_BILLING_STATEMENT ||
              request.descriptor === GENERATE_EARNINGS_STATEMENT
            ) {
              this.request = request;
            } else {
              throw new Error("Not handled.");
            }
          }
        })();

        // 2024-11-04 18:xx:xx UTC
        await new SyncMeterReadingHandler(
          BIGTABLE,
          clientMock,
          () => new Date(1730745230000),
        ).handle(
          "",
          {
            seasonId: "season1",
            episodeId: "ep1",
            watchTimeMs: 12300000,
          },
          "session1",
        );

        // 2024-11-05 18:xx:xx UTC
        let consumerDailyBatchResponse = await new ConsumerGetDailyBatchHandler(
          10,
          BIGTABLE,
          () => new Date(1730831630000),
        ).handle("", {});
        assertThat(
          consumerDailyBatchResponse.rowKeys,
          isArray([eq("t1#2024-11-04#consumer1")]),
          "consumer daily batch",
        );

        await new ConsumerProcessDailyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        ).handle("", {
          rowKey: consumerDailyBatchResponse.rowKeys[0],
        });

        // 2024-11-05 18:xx:xx UTC
        let consumerListPerSeasonResponse =
          await new ConsumerListMeterReadingPerSeasonHandler(
            BIGTABLE,
            clientMock,
            () => new Date(1730831630000),
          ).handle("", {}, "session1");
        assertThat(
          consumerListPerSeasonResponse,
          eqMessage(
            {
              readings: [
                {
                  season: {
                    seasonId: "season1",
                    seasonName: "name1",
                  },
                  watchTimeSec: 12300,
                },
              ],
            },
            CONSUMER_LIST_METER_READING_PER_SEASON_RESPONSE,
          ),
          "consumer list per season",
        );

        let consumerListPerDayResponse =
          await new ConsumerListMeterReadingsPerDayHandler(
            BIGTABLE,
            clientMock,
          ).handle(
            "",
            {
              startDate: "2024-11-04",
              endDate: "2024-11-05",
            },
            "session1",
          );
        assertThat(
          consumerListPerDayResponse,
          eqMessage(
            {
              readings: [
                {
                  date: "2024-11-04",
                  watchTimeSec: 61500,
                },
              ],
            },
            CONSUMER_LIST_METER_READINGS_PER_DAY_RESPONSE,
          ),
          "consumer list per day",
        );

        // 2024-11-05 18:xx:xx UTC
        let publisherDailyBatchResponse =
          await new PublisherGetDailyBatchHandler(
            10,
            BIGTABLE,
            () => new Date(1730831630000),
          ).handle("", {});
        assertThat(
          publisherDailyBatchResponse.rowKeys,
          isArray([eq("t4#2024-11-04#publisher1")]),
          "publisher daily batch",
        );

        await new PublisherProcessDailyMeterReadingHandler(10, BIGTABLE).handle(
          "",
          {
            rowKey: publisherDailyBatchResponse.rowKeys[0],
          },
        );

        // 2024-12-05 18:xx:xx UTC
        let consumerMonthlyBatchResponse =
          await new ConsumerGetMonthlyBatchHandler(
            10,
            BIGTABLE,
            () => new Date(1733423630000),
          ).handle("", {});
        assertThat(
          consumerMonthlyBatchResponse.rowKeys,
          isArray([eq("t6#2024-11#consumer1")]),
          "consumer monthly batch",
        );

        await new ConsumerProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        ).handle("", {
          rowKey: consumerMonthlyBatchResponse.rowKeys[0],
        });
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              accountId: "consumer1",
              month: "2024-11",
              readings: [
                {
                  meterType: ConsumerMeterType.SHOW_WATCH_TIME_SEC,
                  reading: 61500,
                },
              ],
            },
            GENERATE_BILLING_STATEMENT_REQUEST_BODY,
          ),
          "generating billing request",
        );

        let consumerListPerMonthResponse =
          await new ConsumerListMeterReadingsPerMonthHandler(
            BIGTABLE,
            clientMock,
          ).handle(
            "",
            { startMonth: "2024-11", endMonth: "2024-12" },
            "session1",
          );
        assertThat(
          consumerListPerMonthResponse,
          eqMessage(
            {
              readings: [
                {
                  month: "2024-11",
                  watchTimeSec: 61500,
                },
              ],
            },
            CONSUMER_LIST_METER_READINGS_PER_MONTH_RESPONSE,
          ),
          "consumer list per month",
        );

        // 2024-12-05 18:xx:xx UTC
        await new LoadPublishersToProcessMonthlyHandler(
          "2024-10",
          10,
          BIGTABLE,
          clientMock,
          () => new Date(1733423630000),
        ).handle("", {});

        // 2024-12-05 18:xx:xx UTC
        let publisherMonthlyBatchResponse =
          await new PublisherGetMonthlyBatchHandler(
            10,
            BIGTABLE,
            () => new Date(1733423630000),
          ).handle("", {});
        assertThat(
          publisherMonthlyBatchResponse.rowKeys,
          isArray([eq("t7#2024-11#publisher1")]),
          "publisher monthly batch",
        );

        await new PublisherProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        ).handle("", {
          rowKey: publisherMonthlyBatchResponse.rowKeys[0],
        });
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              accountId: "publisher1",
              month: "2024-11",
              readings: [
                {
                  meterType: PublisherMeterType.SHOW_WATCH_TIME_SEC,
                  reading: 61500,
                },
                {
                  meterType: PublisherMeterType.NETWORK_TRANSMITTED_MB,
                  reading: 8,
                },
                {
                  meterType: PublisherMeterType.STORAGE_MB_HOUR,
                  reading: 132,
                },
                {
                  meterType: PublisherMeterType.UPLOAD_MB,
                  reading: 332,
                },
              ],
            },
            GENERATE_EARNINGS_STATEMENT_REQUEST_BODY,
          ),
          "generating earnings request",
        );
      },
      tearDown: async () => {
        await Promise.all([
          BIGTABLE.deleteRows("t"),
          BIGTABLE.deleteRows("f"),
          BIGTABLE.deleteRows("l"),
        ]);
      },
    },
  ],
});
