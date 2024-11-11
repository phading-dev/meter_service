import { BIGTABLE } from "./common/bigtable";
import { GetDailyBatchHandler as ConsumerGetDailyBatchHandler } from "./show/backend/consumer/get_daily_batch_handler";
import { GetMonthlyBatchHandler as ConsumerGetMonthlyBatchHandler } from "./show/backend/consumer/get_monthly_batch_handler";
import { ProcessDailyMeterReadingHandler as ConsumerProcessDailyMeterReadingHandler } from "./show/backend/consumer/process_daily_meter_reading_handler";
import { ProcessMonthlyMeterReadingHandler as ConsumerProcessMonthlyMeterReadingHandler } from "./show/backend/consumer/process_monthly_meter_reading_handler";
import { GetDailyBatchHandler as PublisherGetDailyBatchHandler } from "./show/backend/publisher/get_daily_batch_handler";
import { GetMonthlyBatchHandler as PublisherGetMonthlyBatchHandler } from "./show/backend/publisher/get_monthly_batch_handler";
import { LoadPublishersToProcessMonthlyHandler } from "./show/backend/publisher/load_publishers_to_process_monthly";
import { ProcessDailyMeterReadingHandler as PublisherProcessDailyMeterReadingHandler } from "./show/backend/publisher/process_daily_meter_reading_handler";
import { ProcessMonthlyMeterReadingHandler as PublisherProcessMonthlyMeterReadingHandler } from "./show/backend/publisher/process_monthly_meter_reading_handler";
import { ListMeterReadingsPerDayHandler as ConsumerListMeterReadingsPerDayHandler } from "./show/frontend/consumer/list_meter_reading_per_day_handler";
import { ListMeterReadingsPerMonthHandler as ConsumerListMeterReadingsPerMonthHandler } from "./show/frontend/consumer/list_meter_reading_per_month_handler";
import { ListMeterReadingPerSeasonHandler as ConsumerListMeterReadingPerSeasonHandler } from "./show/frontend/consumer/list_meter_reading_per_season_handler";
import { SyncMeterReadingHandler } from "./show/frontend/consumer/sync_meter_reading_handler";
import { ListMeterReadingsPerDayHandler as PublisherListMeterReadingsPerDayHandler } from "./show/frontend/publisher/list_meter_reading_per_day_handler";
import { ListMeterReadingsPerMonthHandler as PublisherListMeterReadingsPerMonthHandler } from "./show/frontend/publisher/list_meter_reading_per_month_handler";
import { ListMeterReadingPerSeasonHandler as PublisherListMeterReadingPerSeasonHandler } from "./show/frontend/publisher/list_meter_reading_per_season_handler";
import {
  GENERATE_BILLING_STATEMENT,
  GENERATE_BILLING_STATEMENT_REQUEST_BODY,
  MeterType as ConsumerMeterType,
} from "@phading/commerce_service_interface/backend/consumer/interface";
import {
  GENERATE_EARNINGS_STATEMENT,
  GENERATE_EARNINGS_STATEMENT_REQUEST_BODY,
  MeterType as PublisherMeterType,
} from "@phading/commerce_service_interface/backend/publisher/interface";
import {
  LIST_METER_READINGS_PER_DAY_RESPONSE as CONSUMER_LIST_METER_READINGS_PER_DAY_RESPONSE,
  LIST_METER_READINGS_PER_MONTH_RESPONSE as CONSUMER_LIST_METER_READINGS_PER_MONTH_RESPONSE,
  LIST_METER_READING_PER_SEASON_RESPONSE as CONSUMER_LIST_METER_READING_PER_SEASON_RESPONSE,
} from "@phading/product_meter_service_interface/show/frontend/consumer/interface";
import {
  LIST_METER_READINGS_PER_DAY_RESPONSE as PUBLISHER_LIST_METER_READINGS_PER_DAY_RESPONSE,
  LIST_METER_READINGS_PER_MONTH_RESPONSE as PUBLISHER_LIST_METER_READINGS_PER_MONTH_RESPONSE,
  LIST_METER_READING_PER_SEASON_RESPONSE as PUBLISHER_LIST_METER_READING_PER_SEASON_RESPONSE,
} from "@phading/product_meter_service_interface/show/frontend/publisher/interface";
import {
  GET_SEASON_NAME,
  GET_SEASON_PUBLISHER_AND_GRADE,
  GET_STORAGE_METER_READING,
  GET_UPLOAD_METER_READING,
  GET_VIDEO_DURATION_AND_SIZE,
  GetSeasonNameResponse,
  GetSeasonPublisherAndGradeResponse,
  GetStorageMeterReadingResponse,
  GetUploadMeterReadingResponse,
  GetVideoDurationAndSizeResponse,
} from "@phading/product_service_interface/show/backend/interface";
import {
  LIST_ACCOUNTS,
  ListAccountsResponse,
} from "@phading/user_service_interface/backend/interface";
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
              if (request.body.signedSession === "consumerSession1") {
                return {
                  accountId: "consumer1",
                  canConsumeShows: true,
                } as ExchangeSessionAndCheckCapabilityResponse;
              } else {
                return {
                  accountId: "publisher1",
                  canPublishShows: true,
                } as ExchangeSessionAndCheckCapabilityResponse;
              }
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
          "consumerSession1",
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
          ).handle("", {}, "consumerSession1");
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
                  watchTimeSecGraded: 61500,
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
            "consumerSession1",
          );
        assertThat(
          consumerListPerDayResponse,
          eqMessage(
            {
              readings: [
                {
                  date: "2024-11-04",
                  watchTimeSecGraded: 61500,
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

        // 2024-11-05 18:xx:xx UTC
        let publisherListPerSeasonResponse =
          await new PublisherListMeterReadingPerSeasonHandler(
            BIGTABLE,
            clientMock,
            () => new Date(1730831630000),
          ).handle("", {}, "publisherSession1");
        assertThat(
          publisherListPerSeasonResponse,
          eqMessage(
            {
              readings: [
                {
                  season: {
                    seasonId: "season1",
                    seasonName: "name1",
                  },
                  watchTimeSec: 12300,
                  watchTimeSecGraded: 61500,
                },
              ],
            },
            PUBLISHER_LIST_METER_READING_PER_SEASON_RESPONSE,
          ),
          "publisher list per season",
        );

        let publisherListPerDayResponse =
          await new PublisherListMeterReadingsPerDayHandler(
            BIGTABLE,
            clientMock,
          ).handle(
            "",
            {
              startDate: "2024-11-04",
              endDate: "2024-11-05",
            },
            "publisherSession1",
          );
        assertThat(
          publisherListPerDayResponse,
          eqMessage(
            {
              readings: [
                {
                  date: "2024-11-04",
                  watchTimeSecGraded: 61500,
                  transmittedKb: 7208,
                },
              ],
            },
            PUBLISHER_LIST_METER_READINGS_PER_DAY_RESPONSE,
          ),
          "publisher list per day",
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
            "consumerSession1",
          );
        assertThat(
          consumerListPerMonthResponse,
          eqMessage(
            {
              readings: [
                {
                  month: "2024-11",
                  watchTimeSecGraded: 61500,
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

        let publisherListPerMonthResponse =
          await new PublisherListMeterReadingsPerMonthHandler(
            BIGTABLE,
            clientMock,
          ).handle(
            "",
            { startMonth: "2024-11", endMonth: "2024-12" },
            "publisherSession1",
          );
        assertThat(
          publisherListPerMonthResponse,
          eqMessage(
            {
              readings: [
                {
                  month: "2024-11",
                  watchTimeSecGraded: 61500,
                  transmittedMb: 8,
                  storageMbh: 132,
                  uploadMb: 332,
                },
              ],
            },
            PUBLISHER_LIST_METER_READINGS_PER_MONTH_RESPONSE,
          ),
          "publisher list per month",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("f");
        await BIGTABLE.deleteRows("l");
      },
    },
  ],
});
