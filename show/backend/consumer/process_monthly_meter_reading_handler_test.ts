import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { ProcessMonthlyMeterReadingHandler } from "./process_monthly_meter_reading_handler";
import {
  GENERATE_BILLING_STATEMENT,
  GENERATE_BILLING_STATEMENT_REQUEST_BODY,
  MeterType,
} from "@phading/commerce_service_interface/backend/consumer/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function initData() {
  await BIGTABLE.insert([
    {
      key: "q2#2024-10#consumer1",
      data: {
        c: {
          p: {
            value: "",
          },
        },
      },
    },
    {
      key: "d2#2024-10#consumer1#01",
      data: {
        t: {
          w: {
            value: 100,
          },
        },
      },
    },
    {
      key: "d2#2024-10#consumer1#18",
      data: {
        t: {
          w: {
            value: 300,
          },
        },
      },
    },
    {
      key: "d2#2024-10#consumer1#20",
      data: {
        t: {
          w: {
            value: 500,
          },
        },
      },
    },
    {
      key: "d2#2024-10#consumer2#20",
      data: {
        t: {
          w: {
            value: 500,
          },
        },
      },
    },
  ]);
}

TEST_RUNNER.run({
  name: "ProcessMonthlyMeterReadingHandlerTest",
  cases: [
    {
      name: "ProcssedInOneShot_ProcessedAgainWithNoAction",
      execute: async () => {
        // Prepare
        await initData();
        let clientMock = new NodeServiceClientMock();
        clientMock.response = {};
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );

        // Execute
        await handler.handle("", {
          rowKey: "q2#2024-10#consumer1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f2#consumer1#2024-10").get())[0].data,
          eqData({
            t: {
              w: {
                value: 900,
              },
            },
          }),
          "final consumer month data",
        );
        assertThat(
          (await BIGTABLE.row("q2#2024-10#consumer1").exists())[0],
          eq(false),
          "consumer month queue deleted",
        );
        assertThat(
          clientMock.request.descriptor,
          eq(GENERATE_BILLING_STATEMENT),
          "RC descriptor",
        );
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              accountId: "consumer1",
              month: "2024-10",
              readings: [
                {
                  meterType: MeterType.SHOW_WATCH_TIME_SEC,
                  reading: 900,
                },
              ],
            },
            GENERATE_BILLING_STATEMENT_REQUEST_BODY,
          ),
          "generate billing request",
        );

        // Execute
        await handler.handle("", {
          rowKey: "q2#2024-10#consumer1",
        });

        // Verify no error and no actions
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("q");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
  ],
});
