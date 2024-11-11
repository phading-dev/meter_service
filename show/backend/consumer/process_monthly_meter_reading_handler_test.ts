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
import {
  assertReject,
  assertThat,
  containStr,
  eq,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function initData() {
  await BIGTABLE.insert([
    {
      key: "t6#2024-10#consumer1",
      data: {
        t: {
          w: {
            value: 0,
          },
        },
        c: {
          p: {
            value: "",
          },
        },
      },
    },
    {
      key: "t2#2024-10#consumer1#01",
      data: {
        t: {
          w: {
            value: 100,
          },
        },
      },
    },
    {
      key: "t2#2024-10#consumer1#18",
      data: {
        t: {
          w: {
            value: 300,
          },
        },
      },
    },
    {
      key: "t2#2024-10#consumer1#20",
      data: {
        t: {
          w: {
            value: 500,
          },
        },
      },
    },
    {
      key: "t2#2024-10#consumer2#20",
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
      name: "ProcssedInOneShot",
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
          rowKey: "t6#2024-10#consumer1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f3#consumer1#2024-10").get())[0].data,
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
          (await BIGTABLE.row("t6#2024-10#consumer1").exists())[0],
          eq(false),
          "original row deleted",
        );
        assertThat(
          (await BIGTABLE.row("t2#2024-10#consumer1#20").exists())[0],
          eq(false),
          "one data row deleted",
        );
        assertThat(
          (await BIGTABLE.row("t2#2024-10#consumer2#20").exists())[0],
          eq(true),
          "extra data row exists",
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
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "InterruptAfterCheckPoint_ResumeAndMarkDone_ResumeWithNoAction",
      execute: async () => {
        // Prepare
        await initData();
        let clientMock = new NodeServiceClientMock();
        clientMock.response = {};
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );
        handler.interruptAfterCheckPoint = () => {
          throw new Error("fake error");
        };

        // Execute
        let error = await assertReject(
          handler.handle("", {
            rowKey: "t6#2024-10#consumer1",
          }),
        );

        // Verify
        assertThat(error.message, containStr("fake"), "error");
        assertThat(
          (await BIGTABLE.row("t6#2024-10#consumer1").get())[0].data,
          eqData({
            t: {
              w: {
                value: 900,
              },
            },
            c: {
              p: {
                value: "1",
              },
            },
          }),
          "checkpoint data",
        );
        assertThat(clientMock.request, eq(undefined), "no RC");

        // Execute
        await handler.handle("", {
          rowKey: "t6#2024-10#consumer1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f3#consumer1#2024-10").get())[0].data,
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
          (await BIGTABLE.row("t6#2024-10#consumer1").exists())[0],
          eq(false),
          "original row deleted",
        );
        assertThat(
          (await BIGTABLE.row("t2#2024-10#consumer1#20").exists())[0],
          eq(false),
          "one data row deleted",
        );
        assertThat(
          (await BIGTABLE.row("t2#2024-10#consumer2#20").exists())[0],
          eq(true),
          "extra data row exists",
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
          rowKey: "t6#2024-10#consumer1",
        });

        // Verify no error and no actions
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "SimulatedWriteConflict",
      execute: async () => {
        // Prepare
        await initData();
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );
        handler.interfereBeforeCheckPoint = async () => {
          await BIGTABLE.insert([
            {
              key: "t6#2024-10#consumer1",
              data: {
                t: {
                  w: {
                    value: 100,
                  },
                },
                c: {
                  p: {
                    value: "1",
                  },
                },
              },
            },
          ]);
        };

        // Execute
        let error = await assertReject(
          handler.handle("", {
            rowKey: "t6#2024-10#consumer1",
          }),
        );

        // Verify
        assertThat(
          error.message,
          containStr("Row t6#2024-10#consumer1 is already completed"),
          "error",
        );
        assertThat(
          (await BIGTABLE.row("t6#2024-10#consumer1").get())[0].data,
          eqData({
            t: {
              w: {
                value: 100,
              },
            },
            c: {
              p: {
                value: "1",
              },
            },
          }),
          "checkpoint data",
        );
        assertThat(clientMock.request, eq(undefined), "no RC");
        assertThat(
          (await BIGTABLE.row("f3#consumer1#2024-10").exists())[0],
          eq(false),
          "final data not written",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("f");
      },
    },
  ],
});
