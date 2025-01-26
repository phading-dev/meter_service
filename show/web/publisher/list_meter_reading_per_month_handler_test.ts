import { BIGTABLE } from "../../../common/bigtable";
import { ListMeterReadingsPerMonthHandler } from "./list_meter_reading_per_month_handler";
import { LIST_METER_READINGS_PER_MONTH_RESPONSE } from "@phading/product_meter_service_interface/show/web/publisher/interface";
import { ExchangeSessionAndCheckCapabilityResponse } from "@phading/user_session_service_interface/node/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListMeterReadingsPerMonthHandlerTest",
  cases: [
    {
      name: "Default",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "f4#publisher1#2024-09",
            data: {
              t: {
                ws: {
                  value: 100,
                },
                nm: {
                  value: 10,
                },
                um: {
                  value: 11,
                },
                smh: {
                  value: 111,
                },
              },
            },
          },
          {
            key: "f4#publisher1#2024-10",
            data: {
              t: {
                ws: {
                  value: 200,
                },
                nm: {
                  value: 20,
                },
                um: {
                  value: 22,
                },
                smh: {
                  value: 222,
                },
              },
            },
          },
          {
            key: "f4#publisher1#2024-12",
            data: {
              t: {
                ws: {
                  value: 300,
                },
                nm: {
                  value: 30,
                },
              },
            },
          },
          {
            key: "f4#publisher1#2025-01",
            data: {
              t: {
                um: {
                  value: 44,
                },
                smh: {
                  value: 444,
                },
              },
            },
          },
          {
            key: "f4#publisher1#2025-02",
            data: {
              t: {
                ws: {
                  value: 500,
                },
              },
            },
          },
        ]);
        let clientMock = new NodeServiceClientMock();
        clientMock.response = {
          accountId: "publisher1",
          capabilities: {
            canPublishShows: true,
          },
        } as ExchangeSessionAndCheckCapabilityResponse;
        let handler = new ListMeterReadingsPerMonthHandler(
          BIGTABLE,
          clientMock,
        );

        // Execute
        let resposne = await handler.handle(
          "",
          { startMonth: "2024-10", endMonth: "2025-01" },
          "session1",
        );

        // Verify
        assertThat(
          resposne,
          eqMessage(
            {
              readings: [
                {
                  month: "2024-10",
                  watchTimeSecGraded: 200,
                  transmittedMb: 20,
                  uploadedMb: 22,
                  storageMbh: 222,
                },
                {
                  month: "2024-12",
                  watchTimeSecGraded: 300,
                  transmittedMb: 30,
                },
                {
                  month: "2025-01",
                  uploadedMb: 44,
                  storageMbh: 444,
                },
              ],
            },
            LIST_METER_READINGS_PER_MONTH_RESPONSE,
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
