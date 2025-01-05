import { BIGTABLE } from "./common/bigtable";
import { GetDailyBatchHandler as ConsumerGetDailyBatchHandler } from "./show/node/consumer/get_daily_batch_handler";
import { GetMonthlyBatchHandler as ConsumerGetMonthlyBatchHandler } from "./show/node/consumer/get_monthly_batch_handler";
import { ProcessDailyMeterReadingHandler as ConsumerProcessDailyMeterReadingHandler } from "./show/node/consumer/process_daily_meter_reading_handler";
import { ProcessMonthlyMeterReadingHandler as ConsumerProcessMonthlyMeterReadingHandler } from "./show/node/consumer/process_monthly_meter_reading_handler";
import { GetDailyStorageBatchHandler as PublisherGetDailyStorageBatchHandler } from "./show/node/publisher/get_daily_storage_batch_handler";
import { GetDailyWatchBatchHandler as PublisherGetDailyWatchBatchHandler } from "./show/node/publisher/get_daily_watch_batch_handler";
import { GetMonthlyBatchHandler as PublisherGetMonthlyBatchHandler } from "./show/node/publisher/get_monthly_batch_handler";
import { ProcessDailyStorageReadingHandler as PublisherProcessDailyStorageReadingHandler } from "./show/node/publisher/process_daily_storage_reading_handler";
import { ProcessDailyWatchReadingHandler as PublisherProcessDailyWatchReadingHandler } from "./show/node/publisher/process_daily_watch_reading_handler";
import { ProcessMonthlyMeterReadingHandler as PublisherProcessMonthlyMeterReadingHandler } from "./show/node/publisher/process_monthly_meter_reading_handler";
import { CachedSessionExchanger } from "./show/web/consumer/common/cached_session_exchanger";
import { ListMeterReadingsPerDayHandler as ConsumerListMeterReadingsPerDayHandler } from "./show/web/consumer/list_meter_reading_per_day_handler";
import { ListMeterReadingsPerMonthHandler as ConsumerListMeterReadingsPerMonthHandler } from "./show/web/consumer/list_meter_reading_per_month_handler";
import { ListMeterReadingPerSeasonHandler as ConsumerListMeterReadingPerSeasonHandler } from "./show/web/consumer/list_meter_reading_per_season_handler";
import { RecordNetworkTransmissionHandler } from "./show/web/consumer/record_network_transmission_handler";
import { RecordWatchTimeHandler } from "./show/web/consumer/record_watch_time_handler";
import { ListMeterReadingsPerDayHandler as PublisherListMeterReadingsPerDayHandler } from "./show/web/publisher/list_meter_reading_per_day_handler";
import { ListMeterReadingsPerMonthHandler as PublisherListMeterReadingsPerMonthHandler } from "./show/web/publisher/list_meter_reading_per_month_handler";
import { ListMeterReadingPerSeasonHandler as PublisherListMeterReadingPerSeasonHandler } from "./show/web/publisher/list_meter_reading_per_season_handler";
import { RecordStorageEndHandler } from "./show/web/publisher/record_storage_end_handler";
import { RecordStorageStartHandler } from "./show/web/publisher/record_storage_start_handler";
import { RecordUploadedHandler } from "./show/web/publisher/record_uploaded_handler";
import {
  GENERATE_BILLING_STATEMENT,
  GENERATE_BILLING_STATEMENT_REQUEST_BODY,
  MeterType as ConsumerMeterType,
} from "@phading/commerce_service_interface/node/consumer/interface";
import {
  GENERATE_EARNINGS_STATEMENT,
  GENERATE_EARNINGS_STATEMENT_REQUEST_BODY,
  MeterType as PublisherMeterType,
} from "@phading/commerce_service_interface/node/publisher/interface";
import {
  LIST_METER_READINGS_PER_DAY_RESPONSE as CONSUMER_LIST_METER_READINGS_PER_DAY_RESPONSE,
  LIST_METER_READINGS_PER_MONTH_RESPONSE as CONSUMER_LIST_METER_READINGS_PER_MONTH_RESPONSE,
  LIST_METER_READING_PER_SEASON_RESPONSE as CONSUMER_LIST_METER_READING_PER_SEASON_RESPONSE,
} from "@phading/product_meter_service_interface/show/web/consumer/interface";
import {
  LIST_METER_READINGS_PER_DAY_RESPONSE as PUBLISHER_LIST_METER_READINGS_PER_DAY_RESPONSE,
  LIST_METER_READINGS_PER_MONTH_RESPONSE as PUBLISHER_LIST_METER_READINGS_PER_MONTH_RESPONSE,
  LIST_METER_READING_PER_SEASON_RESPONSE as PUBLISHER_LIST_METER_READING_PER_SEASON_RESPONSE,
} from "@phading/product_meter_service_interface/show/web/publisher/interface";
import {
  GET_SEASON_GRADE,
  GET_SEASON_PUBLISHER,
  GetSeasonGradeResponse,
  GetSeasonPublisherResponse,
} from "@phading/product_service_interface/show/node/interface";
import {
  EXCHANGE_SESSION_AND_CHECK_CAPABILITY,
  ExchangeSessionAndCheckCapabilityResponse,
} from "@phading/user_session_service_interface/node/interface";
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
            } else if (request.descriptor === GET_SEASON_PUBLISHER) {
              return {
                publisherId: "publisher1",
              } as GetSeasonPublisherResponse;
            } else if (request.descriptor === GET_SEASON_GRADE) {
              return {
                grade: 5,
              } as GetSeasonGradeResponse;
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

        // 2024-11-04T18:00:00Z
        await new RecordWatchTimeHandler(
          BIGTABLE,
          new CachedSessionExchanger(clientMock),
          () => new Date(1730743200000),
        ).handle(
          "",
          {
            seasonId: "season1",
            episodeId: "ep1",
            watchTimeMs: 12300000,
          },
          "consumerSession1",
        );

        // 2024-11-04T18:00:00Z
        await new RecordNetworkTransmissionHandler(
          BIGTABLE,
          new CachedSessionExchanger(clientMock),
          () => new Date(1730743200000),
        ).handle(
          "",
          {
            seasonId: "season1",
            episodeId: "ep1",
            transmittedBytes: 1024000000,
          },
          "consumerSession1",
        );

        // 2024-11-05T18:00:00Z
        let consumerDailyBatchResponse = await new ConsumerGetDailyBatchHandler(
          10,
          BIGTABLE,
          () => new Date(1730829600000),
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

        // 2024-11-05T18:00:00Z
        let consumerListPerSeasonResponse =
          await new ConsumerListMeterReadingPerSeasonHandler(
            BIGTABLE,
            clientMock,
            () => new Date(1730829600000),
          ).handle("", {}, "consumerSession1");
        assertThat(
          consumerListPerSeasonResponse,
          eqMessage(
            {
              readings: [
                {
                  seasonId: "season1",
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

        // 2024-11-05T18:00:00Z
        let publisherDailyWatchBatchResponse =
          await new PublisherGetDailyWatchBatchHandler(
            10,
            BIGTABLE,
            () => new Date(1730829600000),
          ).handle("", {});
        assertThat(
          publisherDailyWatchBatchResponse.rowKeys,
          isArray([eq("t3#2024-11-04#publisher1")]),
          "publisher daily watch batch",
        );

        let checkpointId = 0;
        await new PublisherProcessDailyWatchReadingHandler(
          10,
          BIGTABLE,
          () => `${checkpointId++}`,
        ).handle("", {
          rowKey: publisherDailyWatchBatchResponse.rowKeys[0],
        });

        // 2024-11-04T18:00:00Z
        await new RecordUploadedHandler(
          BIGTABLE,
          clientMock,
          () => new Date(1730743200000),
        ).handle(
          "",
          {
            name: "rawVideo1",
            uploadedBytes: 2345000,
          },
          "publisherSession1",
        );

        // 2024-11-04T18:00:00Z
        await new RecordStorageStartHandler(
          BIGTABLE,
          clientMock,
          () => new Date(1730743200000),
        ).handle(
          "",
          {
            name: "video1",
            storageBytes: 2500000,
            storageStartMs: 1730743200000,
          },
          "publisherSession1",
        );

        // 2024-11-05T07:00:00Z
        await new RecordStorageEndHandler(
          BIGTABLE,
          clientMock,
          () => new Date(1730790000000),
        ).handle(
          "",
          {
            name: "video1",
            storageEndMs: 1730790000000,
          },
          "publisherSession1",
        );

        // 2024-11-05T18:00:00Z
        let publisherDailyStorageBatchResponse =
          await new PublisherGetDailyStorageBatchHandler(
            10,
            BIGTABLE,
            () => new Date(1730829600000),
          ).handle("", {});
        assertThat(
          publisherDailyStorageBatchResponse.rowKeys,
          isArray([eq("t6#2024-11-04#publisher1")]),
          "publisher daily storage batch",
        );

        await new PublisherProcessDailyStorageReadingHandler(BIGTABLE).handle(
          "",
          {
            rowKey: publisherDailyStorageBatchResponse.rowKeys[0],
          },
        );

        // 2024-11-05T18:00:00Z
        let publisherListPerSeasonResponse =
          await new PublisherListMeterReadingPerSeasonHandler(
            BIGTABLE,
            clientMock,
            () => new Date(1730829600000),
          ).handle("", {}, "publisherSession1");
        assertThat(
          publisherListPerSeasonResponse,
          eqMessage(
            {
              readings: [
                {
                  seasonId: "season1",
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
                  transmittedKb: 1000000,
                  uploadedKb: 2291,
                  storageMbm: 1860,
                },
              ],
            },
            PUBLISHER_LIST_METER_READINGS_PER_DAY_RESPONSE,
          ),
          "publisher list per day",
        );

        // 2024-12-05T18:00:00Z
        let consumerMonthlyBatchResponse =
          await new ConsumerGetMonthlyBatchHandler(
            10,
            BIGTABLE,
            () => new Date(1733421600000),
          ).handle("", {});
        assertThat(
          consumerMonthlyBatchResponse.rowKeys,
          isArray([eq("t2#2024-11#consumer1")]),
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

        // 2024-12-05T18:00:00Z
        let publisherMonthlyBatchResponse =
          await new PublisherGetMonthlyBatchHandler(
            10,
            BIGTABLE,
            () => new Date(1733421600000),
          ).handle("", {});
        assertThat(
          publisherMonthlyBatchResponse.rowKeys,
          isArray([eq("t5#2024-11#publisher1")]),
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
                  reading: 977,
                },
                {
                  meterType: PublisherMeterType.UPLOADED_MB,
                  reading: 3,
                },
                {
                  meterType: PublisherMeterType.STORAGE_MB_HOUR,
                  reading: 31,
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
                  transmittedMb: 977,
                  uploadedMb: 3,
                  storageMbh: 31,
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
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
  ],
});
