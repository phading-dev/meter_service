import { BIGTABLE } from "../../../common/bigtable";
import { ListMeterReadingsPerDayHandler } from "./list_meter_reading_per_day_handler";
import { LIST_METER_READINGS_PER_DAY_RESPONSE } from "@phading/product_meter_service_interface/show/frontend/consumer/interface";
import { ExchangeSessionAndCheckCapabilityResponse } from "@phading/user_session_service_interface/backend/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListMeterReadingsPerDayHandlerTest",
  cases: [
    {
      name: "Default",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "f1#consumer1#2024-12-01",
            data: {
              a: {
                season1: {
                  value: 200,
                },
              },
              t: {
                w: {
                  value: 100,
                },
              },
            },
          },
          {
            key: "f1#consumer1#2024-12-10",
            data: {
              t: {
                w: {
                  value: 30000,
                },
              },
            },
          },
          {
            key: "f1#consumer1#2024-12-05",
            data: {
              t: {
                w: {
                  value: 2000,
                },
              },
            },
          },
        ]);
        let clientMock = new NodeServiceClientMock();
        clientMock.response = {
          userSession: {
            accountId: "consumer1",
          },
          canConsumeShows: true,
        } as ExchangeSessionAndCheckCapabilityResponse;
        let handler = new ListMeterReadingsPerDayHandler(BIGTABLE, clientMock);

        // Execute
        let response = await handler.handle(
          "",
          {
            startDate: "2024-12-01",
            endDate: "2024-12-10",
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
                  date: "2024-12-01",
                  watchTimeSecGraded: 100,
                },
                {
                  date: "2024-12-05",
                  watchTimeSecGraded: 2000,
                },
                {
                  date: "2024-12-10",
                  watchTimeSecGraded: 30000,
                },
              ],
            },
            LIST_METER_READINGS_PER_DAY_RESPONSE,
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
