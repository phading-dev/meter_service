import { BIGTABLE } from "../../../common/bigtable";
import { ListMeterReadingsPerMonthHandler } from "./list_meter_reading_per_month_handler";
import { LIST_METER_READINGS_PER_MONTH_RESPONSE } from "@phading/product_meter_service_interface/publisher/show/frontend/interface";
import { ExchangeSessionAndCheckCapabilityResponse } from "@phading/user_session_service_interface/backend/interface";
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
                w: {
                  value: 100,
                },
                mb: {
                  value: 10,
                },
                smbh: {
                  value: 111,
                },
                umb: {
                  value: 11,
                },
              },
            },
          },
          {
            key: "f4#publisher1#2024-10",
            data: {
              t: {
                w: {
                  value: 200,
                },
                mb: {
                  value: 20,
                },
                smbh: {
                  value: 222,
                },
                umb: {
                  value: 22,
                },
              },
            },
          },
          {
            key: "f4#publisher1#2024-12",
            data: {
              t: {
                w: {
                  value: 300,
                },
                mb: {
                  value: 30,
                },
                smbh: {
                  value: 333,
                },
                umb: {
                  value: 33,
                },
              },
            },
          },
          {
            key: "f4#publisher1#2025-01",
            data: {
              t: {
                w: {
                  value: 400,
                },
                mb: {
                  value: 40,
                },
                smbh: {
                  value: 444,
                },
                umb: {
                  value: 44,
                },
              },
            },
          },
          {
            key: "f4#publisher1#2025-02",
            data: {
              t: {
                w: {
                  value: 500,
                },
                mb: {
                  value: 50,
                },
                smbh: {
                  value: 555,
                },
                umb: {
                  value: 55,
                },
              },
            },
          },
        ]);
        let clientMock = new NodeServiceClientMock();
        clientMock.response = {
          userSession: {
            accountId: "publisher1",
          },
          canPublishShows: true,
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
                  storageMbh: 222,
                  uploadMb: 22,
                },
                {
                  month: "2024-12",
                  watchTimeSecGraded: 300,
                  transmittedMb: 30,
                  storageMbh: 333,
                  uploadMb: 33,
                },
                {
                  month: "2025-01",
                  watchTimeSecGraded: 400,
                  transmittedMb: 40,
                  storageMbh: 444,
                  uploadMb: 44,
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
