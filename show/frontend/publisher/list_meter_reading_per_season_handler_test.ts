import { BIGTABLE } from "../../../common/bigtable";
import { ListMeterReadingPerSeasonHandler } from "./list_meter_reading_per_season_handler";
import { LIST_METER_READING_PER_SEASON_RESPONSE } from "@phading/product_meter_service_interface/show/frontend/publisher/interface";
import {
  GET_SEASON_NAME,
  GetSeasonNameResponse,
} from "@phading/product_service_interface/show/backend/interface";
import {
  EXCHANGE_SESSION_AND_CHECK_CAPABILITY,
  ExchangeSessionAndCheckCapabilityResponse,
} from "@phading/user_session_service_interface/backend/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListMeterReadingPerSeasonHandlerTest",
  cases: [
    {
      name: "DefaultToYesterday",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "f3#publisher1#2024-12-31",
            data: {
              w: {
                season1: {
                  value: 10,
                },
                season2: {
                  value: 50,
                },
              },
              a: {
                season1: {
                  value: 100,
                },
                season2: {
                  value: 200,
                },
              },
              t: {
                w: {
                  value: 300,
                },
                kb: {
                  value: 1000,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === EXCHANGE_SESSION_AND_CHECK_CAPABILITY) {
              return {
                accountId: "publisher1",
                canPublishShows: true,
              } as ExchangeSessionAndCheckCapabilityResponse;
            } else if (request.descriptor === GET_SEASON_NAME) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    seasonName: "name1",
                  } as GetSeasonNameResponse;
                case "season2":
                  return {
                    seasonName: "name2",
                  } as GetSeasonNameResponse;
                default:
                  throw new Error("Unexpected");
              }
            } else {
              throw new Error("Not handled");
            }
          }
        })();
        // 2025-01-01 20:xx:xx UTC
        let handler = new ListMeterReadingPerSeasonHandler(
          BIGTABLE,
          clientMock,
          () => new Date(1735762358000),
        );

        // Execute
        let response = await handler.handle("", {}, "session1");

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              readings: [
                {
                  season: {
                    seasonId: "season1",
                    seasonName: "name1",
                  },
                  watchTimeSec: 10,
                  watchTimeSecGraded: 100,
                },
                {
                  season: {
                    seasonId: "season2",
                    seasonName: "name2",
                  },
                  watchTimeSec: 50,
                  watchTimeSecGraded: 200,
                },
              ],
            },
            LIST_METER_READING_PER_SEASON_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "InputDate",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "f3#publisher1#2024-11-03",
            data: {
              w: {
                season1: {
                  value: 10,
                },
                season2: {
                  value: 50,
                },
              },
              a: {
                season1: {
                  value: 100,
                },
                season2: {
                  value: 200,
                },
              },
              t: {
                w: {
                  value: 300,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === EXCHANGE_SESSION_AND_CHECK_CAPABILITY) {
              return {
                accountId: "publisher1",
                canPublishShows: true,
              } as ExchangeSessionAndCheckCapabilityResponse;
            } else if (request.descriptor === GET_SEASON_NAME) {
              switch (request.body.seasonId) {
                case "season1":
                  return {
                    seasonName: "name1",
                  } as GetSeasonNameResponse;
                case "season2":
                  return {
                    seasonName: "name2",
                  } as GetSeasonNameResponse;
                default:
                  throw new Error("Unexpected");
              }
            } else {
              throw new Error("Not handled");
            }
          }
        })();
        // 2025-02-02 20:xx:xx UTC
        let handler = new ListMeterReadingPerSeasonHandler(
          BIGTABLE,
          clientMock,
          () => new Date(1738527158000),
        );

        // Execute
        let response = await handler.handle(
          "",
          {
            date: "2024-11-03",
          },
          "session1",
        );

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              readings: [
                {
                  season: {
                    seasonId: "season1",
                    seasonName: "name1",
                  },
                  watchTimeSec: 10,
                  watchTimeSecGraded: 100,
                },
                {
                  season: {
                    seasonId: "season2",
                    seasonName: "name2",
                  },
                  watchTimeSec: 50,
                  watchTimeSecGraded: 200,
                },
              ],
            },
            LIST_METER_READING_PER_SEASON_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "NoMatchingDate",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "f3#publisher1#2024-11-04",
            data: {
              w: {
                season1: {
                  value: 10,
                },
                season2: {
                  value: 50,
                },
              },
              a: {
                season1: {
                  value: 100,
                },
                season2: {
                  value: 200,
                },
              },
              t: {
                w: {
                  value: 300,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === EXCHANGE_SESSION_AND_CHECK_CAPABILITY) {
              return {
                accountId: "publisher1",
                canPublishShows: true,
              } as ExchangeSessionAndCheckCapabilityResponse;
            } else {
              throw new Error("Not handled");
            }
          }
        })();
        // 2025-02-02 20:xx:xx UTC
        let handler = new ListMeterReadingPerSeasonHandler(
          BIGTABLE,
          clientMock,
          () => new Date(1738527158000),
        );

        // Execute
        let response = await handler.handle(
          "",
          {
            date: "2024-11-03",
          },
          "session1",
        );

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              readings: [],
            },
            LIST_METER_READING_PER_SEASON_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "NoSeasons",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "f3#publisher1#2024-11-03",
            data: {
              t: {
                w: {
                  value: 300,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.descriptor === EXCHANGE_SESSION_AND_CHECK_CAPABILITY) {
              return {
                accountId: "publisher1",
                canPublishShows: true,
              } as ExchangeSessionAndCheckCapabilityResponse;
            } else {
              throw new Error("Not handled");
            }
          }
        })();
        // 2025-02-02 20:xx:xx UTC
        let handler = new ListMeterReadingPerSeasonHandler(
          BIGTABLE,
          clientMock,
          () => new Date(1738527158000),
        );

        // Execute
        let response = await handler.handle(
          "",
          {
            date: "2024-11-03",
          },
          "session1",
        );

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              readings: [],
            },
            LIST_METER_READING_PER_SEASON_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("f");
      },
    },
  ],
});
